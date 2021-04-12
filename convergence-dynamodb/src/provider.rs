use crate::exec_scan::DynamoDBScanExecutionPlan;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::datasource::{Statistics, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use rusoto_dynamodb::DynamoDbClient;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum DynamoDBKey {
	Hash(String),
	Composite(String, String),
}

#[derive(Debug, Clone)]
pub struct DynamoDBTableDefinition {
	pub table_name: String,
	pub key: DynamoDBKey,
	pub schema: SchemaRef,
}

impl DynamoDBTableDefinition {
	pub fn new(table_name: impl Into<String>, key: DynamoDBKey, schema: SchemaRef) -> Self {
		Self {
			table_name: table_name.into(),
			key,
			schema,
		}
	}

	pub fn hash_key(&self) -> &str {
		match &self.key {
			DynamoDBKey::Hash(hash) => hash,
			DynamoDBKey::Composite(hash, _) => hash,
		}
	}

	pub fn sort_key(&self) -> Option<&str> {
		match &self.key {
			DynamoDBKey::Hash(_) => None,
			DynamoDBKey::Composite(_, sort) => Some(sort),
		}
	}
}

pub struct DynamoDBTableProvider {
	client: Arc<DynamoDbClient>,
	def: DynamoDBTableDefinition,
}

impl DynamoDBTableProvider {
	pub fn new(client: DynamoDbClient, def: DynamoDBTableDefinition) -> Self {
		Self {
			client: Arc::new(client),
			def,
		}
	}
}

impl TableProvider for DynamoDBTableProvider {
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
		Ok(Arc::new(DynamoDBScanExecutionPlan {
			client: self.client.clone(),
			def: self.def.clone(),
		}))
	}

	fn statistics(&self) -> Statistics {
		Default::default()
	}
}
