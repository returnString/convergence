use crate::exec_scan::DynamoDbScanExecutionPlan;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::datasource::{Statistics, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use rusoto_dynamodb::DynamoDbClient;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum DynamoDbKey {
	Hash(String),
	Composite(String, String),
}

#[derive(Debug, Clone)]
pub struct DynamoDbTableDefinition {
	pub table_name: String,
	pub key: DynamoDbKey,
	pub schema: SchemaRef,
}

impl DynamoDbTableDefinition {
	pub fn new(table_name: impl Into<String>, key: DynamoDbKey, schema: SchemaRef) -> Self {
		Self {
			table_name: table_name.into(),
			key,
			schema,
		}
	}

	pub fn hash_key(&self) -> &str {
		match &self.key {
			DynamoDbKey::Hash(hash) => hash,
			DynamoDbKey::Composite(hash, _) => hash,
		}
	}

	pub fn sort_key(&self) -> Option<&str> {
		match &self.key {
			DynamoDbKey::Hash(_) => None,
			DynamoDbKey::Composite(_, sort) => Some(sort),
		}
	}
}

pub struct DynamoDbTableProvider {
	client: Arc<DynamoDbClient>,
	def: DynamoDbTableDefinition,
}

impl DynamoDbTableProvider {
	pub fn new(client: DynamoDbClient, def: DynamoDbTableDefinition) -> Self {
		Self {
			client: Arc::new(client),
			def,
		}
	}
}

impl TableProvider for DynamoDbTableProvider {
	fn as_any(&self) -> &dyn Any {
		self
	}

	fn schema(&self) -> SchemaRef {
		self.def.schema.clone()
	}

	fn scan(
		&self,
		_projection: &Option<Vec<usize>>,
		_batch_size: usize,
		_filters: &[Expr],
		_limit: Option<usize>,
	) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
		Ok(Arc::new(DynamoDbScanExecutionPlan {
			client: self.client.clone(),
			def: self.def.clone(),
			num_partitions: 1,
		}))
	}

	fn statistics(&self) -> Statistics {
		Default::default()
	}
}
