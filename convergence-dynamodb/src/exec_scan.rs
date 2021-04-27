use crate::items::items_to_record_batch;
use crate::provider::{DynamoDbClientWrapper, DynamoDbTableDefinition};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use rusoto_dynamodb::{DynamoDb, ScanInput};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct DynamoDbScanExecutionPlan {
	pub(crate) client: Arc<DynamoDbClientWrapper>,
	pub(crate) def: DynamoDbTableDefinition,
	pub(crate) num_partitions: usize,
}

#[async_trait]
impl ExecutionPlan for DynamoDbScanExecutionPlan {
	fn as_any(&self) -> &dyn Any {
		self
	}

	fn schema(&self) -> SchemaRef {
		self.def.schema.clone()
	}

	fn output_partitioning(&self) -> Partitioning {
		Partitioning::UnknownPartitioning(self.num_partitions)
	}

	fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
		vec![]
	}

	fn with_new_children(&self, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
		Err(DataFusionError::NotImplemented(
			"ddb execution plan children replacement".to_owned(),
		))
	}

	async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, DataFusionError> {
		let mut last_key = None;
		let mut batches = Vec::new();

		loop {
			let data = self
				.client
				.scan(ScanInput {
					table_name: self.def.table_name.clone(),
					segment: Some(partition as i64),
					total_segments: Some(self.num_partitions as i64),
					exclusive_start_key: last_key,
					..Default::default()
				})
				.await
				.map_err(|err| DataFusionError::Execution(err.to_string()))?;

			let items = &data.items.unwrap_or_default();

			batches.push(Arc::new(items_to_record_batch(items, self.schema())?));

			last_key = data.last_evaluated_key;
			if last_key.is_none() {
				break;
			}
		}

		Ok(Box::pin(SizedRecordBatchStream::new(self.def.schema.clone(), batches)))
	}
}
