// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::{ErrorKind, ObjectStore};
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::format;
use parquet::schema::types::{from_thrift, SchemaDescriptor};
use snafu::ResultExt;

use crate::error;
use crate::error::Result;

// Refer to https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L74-L90
/// Convert [format::FileMetaData] to [ParquetMetaData]
pub fn parse_parquet_metadata(t_file_metadata: format::FileMetaData) -> Result<ParquetMetaData> {
    let schema = from_thrift(&t_file_metadata.schema).context(error::ConvertMetaDataSnafu)?;
    let schema_desc_ptr = Arc::new(SchemaDescriptor::new(schema));

    let mut row_groups = Vec::with_capacity(t_file_metadata.row_groups.len());
    for rg in t_file_metadata.row_groups {
        row_groups.push(
            RowGroupMetaData::from_thrift(schema_desc_ptr.clone(), rg)
                .context(error::ConvertMetaDataSnafu)?,
        );
    }
    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_desc_ptr);

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_desc_ptr,
        column_orders,
    );
    // There may be a problem owing to lacking of column_index and offset_index,
    // if we open page index in the future.
    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

// Port from https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L106-L137
/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
fn parse_column_orders(
    t_column_orders: Option<Vec<format::ColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Option<Vec<ColumnOrder>> {
    match t_column_orders {
        Some(orders) => {
            // Should always be the case
            assert_eq!(
                orders.len(),
                schema_descr.num_columns(),
                "Column order length mismatch"
            );
            let mut res = Vec::with_capacity(schema_descr.num_columns());
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    format::ColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Some(res)
        }
        None => None,
    }
}

/// Fetches data from object store.
/// If the object store supports blocking, use sequence blocking read.
/// Otherwise, use concurrent read.
pub async fn fetch_byte_ranges(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    if object_store.info().full_capability().blocking {
        fetch_ranges_seq(file_path, object_store, ranges).await
    } else {
        fetch_ranges_concurrent(file_path, object_store, ranges).await
    }
}

/// Fetches data from object store sequentially
async fn fetch_ranges_seq(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    let block_object_store = object_store.blocking();
    let file_path = file_path.to_string();
    let ranges = ranges.to_vec();

    let f = move || -> object_store::Result<Vec<Bytes>> {
        ranges
            .into_iter()
            .map(|range| {
                let data = block_object_store
                    .read_with(&file_path)
                    .range(range.start..range.end)
                    .call()?;
                Ok::<_, object_store::Error>(Bytes::from(data))
            })
            .collect::<object_store::Result<Vec<_>>>()
    };

    maybe_spawn_blocking(f).await
}

/// Fetches data from object store concurrently.
async fn fetch_ranges_concurrent(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    // TODO(QuenKar): may merge small ranges to a bigger range to optimize.
    let mut handles = Vec::with_capacity(ranges.len());
    for range in ranges {
        let future_read = object_store.read_with(file_path);
        handles.push(async move {
            let data = future_read.range(range.start..range.end).await?;
            Ok::<_, object_store::Error>(Bytes::from(data))
        });
    }
    let results = futures::future::try_join_all(handles).await?;
    Ok(results)
}

//  Port from https://github.com/apache/arrow-rs/blob/802ed428f87051fdca31180430ddb0ecb2f60e8b/object_store/src/util.rs#L74-L83
/// Takes a function and spawns it to a tokio blocking pool if available
async fn maybe_spawn_blocking<F, T>(f: F) -> object_store::Result<T>
where
    F: FnOnce() -> object_store::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime
            .spawn_blocking(f)
            .await
            .map_err(new_task_join_error)?,
        Err(_) => f(),
    }
}

//  https://github.com/apache/incubator-opendal/blob/7144ab1ca2409dff0c324bfed062ce985997f8ce/core/src/raw/tokio_util.rs#L21-L23
/// Parse tokio error into opendal::Error.
fn new_task_join_error(e: tokio::task::JoinError) -> object_store::Error {
    object_store::Error::new(ErrorKind::Unexpected, "tokio task join failed").set_source(e)
}

/// Returns a sorted list of ranges that cover `raw_ranges`.
///
/// coalesce: the distance between ranges.
///
/// Basic idea:
/// Ranges:\[(a0, b0),(a1, b1),(a2, b2)...\] -> \[(a0, b2),...\],
/// If `a1-b0 <= coalesce` and `a2-b1 <= coalesce`, then merge (a0, b0), (a1, b1), (a2, b2) to (a0, b2).
/// If range > max_range_size, it won't be merged.
pub fn merge_ranges(
    raw_ranges: &[Range<usize>],
    coalesce: usize,
    max_range_size: usize,
) -> Vec<Range<usize>> {
    if raw_ranges.is_empty() {
        return vec![];
    }

    let mut ranges = raw_ranges.to_vec();
    ranges.sort_unstable_by_key(|range| range.start);

    let mut ret = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && range_end - ranges[start_idx].start <= max_range_size
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        let start = ranges[start_idx].start;
        let end = range_end;
        ret.push(start..end);

        start_idx = end_idx;
        end_idx += 1;
    }

    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_ranges() {
        let fetches = merge_ranges(&[], 0, 0);
        assert!(fetches.is_empty());

        let fetches = merge_ranges(&[0..3; 1], 0, 0);
        assert_eq!(fetches, vec![0..3]);

        let fetches = merge_ranges(&[0..2, 3..5], 0, 0);
        assert_eq!(fetches, vec![0..2, 3..5]);

        let fetches = merge_ranges(&[0..1, 1..2], 0, 1);
        assert_eq!(fetches, vec![0..2]);

        let fetches = merge_ranges(&[0..1, 2..72], 1, 4);
        assert_eq!(fetches, vec![0..72]);

        let fetches = merge_ranges(&[0..1, 56..72, 73..75], 1, 512);
        assert_eq!(fetches, vec![0..1, 56..75]);

        let fetches = merge_ranges(&[0..1, 56..72, 73..75], 1, 8);
        assert_eq!(fetches, vec![0..1, 56..72, 73..75]);

        let fetches = merge_ranges(&[0..1, 5..6, 7..9, 2..3, 4..6], 1, 10);
        assert_eq!(fetches, vec![0..9]);

        let fetches = merge_ranges(&[0..1, 5..6, 7..9, 2..3, 4..6], 1, 1);
        assert_eq!(fetches, vec![0..3, 4..6, 5..9]);

        let fetches = merge_ranges(&[0..1, 6..7, 8..9, 10..14, 9..10], 4, 10);
        assert_eq!(fetches, vec![0..1, 6..14]);

        let fetches = merge_ranges(&[1..3, 2..4, 3..5, 10..14], 0, 10);
        assert_eq!(fetches, vec![1..5, 10..14]);

        let fetches = merge_ranges(
            &[
                18781576..18867979,
                18868061..18953346,
                18953430..19038832,
                19038914..19124728,
                19124810..19210947,
                19211031..19297046,
                19297127..19383242,
                19383327..19470108,
                19470191..19556216,
                19556299..19642233,
                19642321..19642849,
                19642918..19644971,
                19645304..19676379,
                19676437..19676510,
            ],
            256,
            512 * 1024,
        );
        assert_eq!(
            fetches,
            vec![18781576..19383242, 19383327..19644971, 19645304..19676510]
        );
    }
}
