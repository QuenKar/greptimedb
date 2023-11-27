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

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{error, info, tracing};
use datafusion::logical_expr;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionEngine;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{RegionId, ScanRequest};

use crate::consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use crate::engine::MetricEngineInner;
use crate::error::{LogicalRegionNotFoundSnafu, MitoReadOperationSnafu, Result};
use crate::utils;

impl MetricEngineInner {
    #[tracing::instrument(skip_all)]
    pub async fn read_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let is_reading_physical_region = self
            .state
            .read()
            .await
            .physical_regions()
            .contains_key(&region_id);

        if is_reading_physical_region {
            info!(
                "Metric region received read request {request:?} on physical region {region_id:?}"
            );
            self.read_physical_region(region_id, request).await
        } else {
            self.read_logical_region(region_id, request).await
        }
    }

    /// Proxy the read request to underlying physical region (mito engine).
    async fn read_physical_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        self.mito
            .handle_query(region_id, request)
            .await
            .context(MitoReadOperationSnafu)
    }

    async fn read_logical_region(
        &self,
        logical_region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let physical_region_id = {
            let state = &self.state.read().await;
            state
                .get_physical_region_id(logical_region_id)
                .with_context(|| {
                    error!("Trying to read an nonexistent region {logical_region_id}");
                    LogicalRegionNotFoundSnafu {
                        region_id: logical_region_id,
                    }
                })?
        };
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let request = self
            .transform_request(physical_region_id, logical_region_id, request)
            .await?;
        self.mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)
    }

    /// Transform the [ScanRequest] from logical region to physical data region.
    async fn transform_request(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        mut request: ScanRequest,
    ) -> Result<ScanRequest> {
        // transform projection
        let physical_projection = if let Some(projection) = &request.projection {
            self.transform_projection(physical_region_id, logical_region_id, projection)
                .await?
        } else {
            self.default_projection(physical_region_id, logical_region_id)
                .await?
        };
        request.projection = Some(physical_projection);

        // add table filter
        request
            .filters
            .push(self.table_id_filter(logical_region_id));

        Ok(request)
    }

    /// Generate a filter on the table id column.
    fn table_id_filter(&self, logical_region_id: RegionId) -> Expr {
        logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .eq(logical_expr::lit(logical_region_id.table_id()))
            .into()
    }

    /// Transform the projection from logical region to physical region.
    ///
    /// This method will not preserve internal columns.
    pub async fn transform_projection(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        origin_projection: &[usize],
    ) -> Result<Vec<usize>> {
        // project on logical columns
        let logical_columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?;

        // generate physical projection
        let mut physical_projection = Vec::with_capacity(origin_projection.len());
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        for logical_proj in origin_projection {
            let column_id = logical_columns[*logical_proj].column_id;
            // Safety: logical columns is a strict subset of physical columns
            physical_projection.push(physical_metadata.column_index_by_id(column_id).unwrap());
        }

        Ok(physical_projection)
    }

    /// Default projection for a logical region. Includes non-internal columns
    pub async fn default_projection(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<usize>> {
        let logical_columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?;
        let mut projection = Vec::with_capacity(logical_columns.len());
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        for logical_col in logical_columns {
            let column_id = logical_col.column_id;
            // Safety: logical columns is a strict subset of physical columns
            projection.push(physical_metadata.column_index_by_id(column_id).unwrap());
        }
        Ok(projection)
    }
}

#[cfg(test)]
mod test {
    use store_api::region_request::RegionRequest;

    use super::*;
    use crate::engine::alter;
    use crate::test_util::{
        alter_logical_region_add_tag_columns, create_logical_region_request, TestEnv,
    };

    #[tokio::test]
    async fn test_transform_scan_req() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();
        let data_region_id = utils::to_data_region_id(physical_region_id);

        // create another logical region
        let logical_region_id2 = RegionId::new(1112345678, 999);
        let create_request =
            create_logical_region_request(&["123", "456", "789"], physical_region_id, "blabla");
        env.metric()
            .handle_request(logical_region_id2, RegionRequest::Create(create_request))
            .await
            .unwrap();

        // add columns to the first logical region
        let alter_request = alter_logical_region_add_tag_columns(&["987", "789", "654", "321"]);
        env.metric()
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // check explicit projection
        let mut scan_req = ScanRequest {
            projection: Some(vec![0, 1, 2, 3, 4, 5, 6]),
            filters: vec![],
            ..Default::default()
        };

        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();

        assert_eq!(scan_req.projection.unwrap(), vec![0, 1, 4, 7, 8, 9, 10]);
        assert_eq!(scan_req.filters.len(), 1);
        assert_eq!(
            scan_req.filters[0],
            logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                .eq(logical_expr::lit(logical_region_id.table_id()))
                .into()
        );

        // check default projection
        let mut scan_req = ScanRequest::default();
        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();
        assert_eq!(scan_req.projection.unwrap(), vec![0, 1, 4, 7, 8, 9, 10]);
    }
}