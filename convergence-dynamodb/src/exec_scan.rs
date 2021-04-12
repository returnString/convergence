use crate::provider::DynamoDBTableDefinition;
use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, ScanInput};
use std::any::Any;
use std::sync::Arc;

pub struct DynamoDBScanExecutionPlan {
	pub client: Arc<DynamoDbClient>,
	pub def: DynamoDBTableDefinition,
	pub num_partitions: usize,
}

impl std::fmt::Debug for DynamoDBScanExecutionPlan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DynamoDBScanExecutionPlan")
			.field("def", &self.def)
			.finish()
	}
}

#[async_trait]
impl ExecutionPlan for DynamoDBScanExecutionPlan {
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

			let mut columns = Vec::new();
			for field in self.def.schema.fields() {
				let array: ArrayRef = match field.data_type() {
					DataType::Utf8 => {
						let mut builder = StringBuilder::new(items.len());
						for item in items {
							let attr = item.get(field.name());
							match attr.and_then(|v| v.s.as_ref()) {
								Some(value) => builder.append_value(value).unwrap(),
								None => builder.append_null().unwrap(),
							}
						}
						Arc::new(builder.finish())
					}
					DataType::Float64 => {
						let mut builder = Float64Builder::new(items.len());
						for item in items {
							let attr = item.get(field.name());
							match attr.and_then(|v| v.n.as_ref()) {
								Some(value) => {
									builder
										.append_value(
											value
												.parse::<f64>()
												.map_err(|err| DataFusionError::Execution(err.to_string()))?,
										)
										.unwrap();
								}
								None => builder.append_null().unwrap(),
							}
						}
						Arc::new(builder.finish())
					}
					_ => unimplemented!(),
				};

				columns.push(array);
			}

			batches.push(Arc::new(RecordBatch::try_new(self.def.schema.clone(), columns)?));

			last_key = data.last_evaluated_key;
			if last_key.is_none() {
				break;
			}
		}

		Ok(Box::pin(SizedRecordBatchStream::new(self.def.schema.clone(), batches)))
	}
}
