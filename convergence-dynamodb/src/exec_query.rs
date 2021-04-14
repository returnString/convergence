use crate::items::items_to_record_batch;
use crate::provider::DynamoDbTableDefinition;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, QueryInput};
use std::any::Any;
use std::sync::Arc;

pub struct DynamoDbQueryExecutionPlan {
	pub client: Arc<DynamoDbClient>,
	pub def: DynamoDbTableDefinition,
	pub hash_value: String,
}

impl std::fmt::Debug for DynamoDbQueryExecutionPlan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DynamoDbQueryExecutionPlan")
			.field("def", &self.def)
			.field("hash_value", &self.hash_value)
			.finish()
	}
}

#[async_trait]
impl ExecutionPlan for DynamoDbQueryExecutionPlan {
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
		let mut last_key = None;
		let mut batches = Vec::new();

		let expression_attribute_names = Some(
			vec![("#hashkey".to_owned(), self.def.hash_key().to_owned())]
				.into_iter()
				.collect(),
		);

		let expression_attribute_values = Some(
			vec![(
				":hashval".to_owned(),
				AttributeValue {
					s: Some(self.hash_value.clone()),
					..Default::default()
				},
			)]
			.into_iter()
			.collect(),
		);

		loop {
			let data = self
				.client
				.query(QueryInput {
					table_name: self.def.table_name.clone(),
					exclusive_start_key: last_key,
					key_condition_expression: Some("#hashkey = :hashval".to_owned()),
					expression_attribute_names: expression_attribute_names.clone(),
					expression_attribute_values: expression_attribute_values.clone(),
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
