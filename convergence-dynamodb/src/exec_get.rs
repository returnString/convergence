use crate::items::items_to_record_batch;
use crate::provider::{DynamoDbClientWrapper, DynamoDbTableDefinition};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use rusoto_dynamodb::{AttributeValue, DynamoDb, GetItemInput};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct DynamoDbGetItemExecutionPlan {
	pub(crate) client: Arc<DynamoDbClientWrapper>,
	pub(crate) def: DynamoDbTableDefinition,
	pub(crate) key: HashMap<String, AttributeValue>,
}

#[async_trait]
impl ExecutionPlan for DynamoDbGetItemExecutionPlan {
	fn as_any(&self) -> &dyn Any {
		self
	}

	fn schema(&self) -> SchemaRef {
		self.def.schema.clone()
	}

	fn output_partitioning(&self) -> Partitioning {
		Partitioning::UnknownPartitioning(1)
	}

	fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
		vec![]
	}

	fn with_new_children(&self, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
		Err(DataFusionError::NotImplemented(
			"ddb execution plan children replacement".to_owned(),
		))
	}

	async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream, DataFusionError> {
		let mut batches = Vec::new();

		let data = self
			.client
			.get_item(GetItemInput {
				table_name: self.def.table_name.clone(),
				key: self.key.clone(),
				..Default::default()
			})
			.await
			.map_err(|err| DataFusionError::Execution(err.to_string()))?;

		if let Some(item) = data.item {
			batches.push(Arc::new(items_to_record_batch(&[item], self.schema())?));
		}

		Ok(Box::pin(SizedRecordBatchStream::new(self.def.schema.clone(), batches)))
	}
}
