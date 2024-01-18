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

use object_store::ObjectStore;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::FOOTER_SIZE;
use snafu::ResultExt;

use crate::error::{self, Result};

/// The estimated size of the footer and metadata need to read from the end of parquet file.
const DEFAULT_PREFETCH_SIZE: u64 = 64 * 1024;

pub(crate) struct MetadataLoader<'a> {
    object_store: ObjectStore,

    file_path: &'a str,

    file_size: Option<u64>,
}

impl<'a> MetadataLoader<'a> {
    pub fn new(
        object_store: ObjectStore,
        file_path: &'a str,
        file_size: Option<u64>,
    ) -> MetadataLoader {
        Self {
            object_store,
            file_path,
            file_size,
        }
    }

    /// Load the metadata of parquet file.
    ///
    /// Read [DEFAULT_PREFETCH_SIZE] from the end of parquet file at first, if File Metadata is in the
    /// read range, decode it and return [ParquetMetaData], otherwise, read again to get the rest of the metadata.
    ///
    /// Parquet File Format:
    /// ```text
    /// ┌───────────────────────────────────┐
    /// |4-byte magic number "PAR1"         |
    /// |───────────────────────────────────|
    /// |Column 1 Chunk 1 + Column Metadata |
    /// |Column 2 Chunk 1 + Column Metadata |
    /// |...                                |
    /// |Column N Chunk M + Column Metadata |
    /// |───────────────────────────────────|
    /// |File Metadata                      |
    /// |───────────────────────────────────|
    /// |4-byte length of file metadata     |
    /// |4-byte magic number "PAR1"         |
    /// └───────────────────────────────────┘
    /// ```
    ///
    pub async fn load(&self) -> Result<ParquetMetaData> {
        let object_store = &self.object_store;
        let path = self.file_path;
        let file_size = match self.file_size {
            Some(n) => n,
            None => object_store
                .stat(path)
                .await
                .context(error::OpenDalSnafu)?
                .content_length(),
        };

        if file_size < FOOTER_SIZE as u64 {
            return error::InvalidParquetSnafu {
                file: path.to_string(),
                reason: "file size is smaller than footer size".to_string(),
            }
            .fail();
        }

        // Prefetch bytes for metadata from the end and process the footer
        let prefetch_size = DEFAULT_PREFETCH_SIZE.min(file_size);
        let buffer = object_store
            .read_with(path)
            .range((file_size - prefetch_size)..file_size)
            .await
            .context(error::OpenDalSnafu)?;
        let buffer_len = buffer.len();

        let mut footer = [0; 8];
        footer.copy_from_slice(&buffer[(buffer_len - FOOTER_SIZE as usize)..]);
        let metadata_len = decode_footer(&footer).map_err(|_| {
            error::InvalidParquetSnafu {
                file: path.to_string(),
                reason: "failed to decode footer".to_string(),
            }
            .build()
        })? as u64;

        if metadata_len + FOOTER_SIZE as u64 > file_size {
            return error::InvalidParquetSnafu {
                file: path.to_string(),
                reason: format!(
                    "the sum of Metadata length {} and Footer size {} is larger than file size {}",
                    metadata_len, FOOTER_SIZE, file_size
                ),
            }
            .fail();
        }

        let footer_len = metadata_len + FOOTER_SIZE as u64;
        if (footer_len as usize) <= buffer_len {
            // The whole metadata is in the first read
            let offset = buffer_len - footer_len as usize;
            let metadata = decode_metadata(&buffer[offset..]).map_err(|_| {
                error::InvalidParquetSnafu {
                    file: path.to_string(),
                    reason: "failed to decode metadata".to_string(),
                }
                .build()
            })?;
            Ok(metadata)
        } else {
            // The metadata is out of buffer, need to read the rest
            let mut data = object_store
                .read_with(path)
                .range((file_size - footer_len)..(file_size - buffer_len as u64))
                .await
                .context(error::OpenDalSnafu)?;
            data.extend(buffer);

            let metadata = decode_metadata(&data).map_err(|_| {
                error::InvalidParquetSnafu {
                    file: path.to_string(),
                    reason: "failed to decode metadata".to_string(),
                }
                .build()
            })?;
            Ok(metadata)
        }
    }
}
