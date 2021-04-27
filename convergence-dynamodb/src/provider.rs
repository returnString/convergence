use crate::exec_get::DynamoDbGetItemExecutionPlan;
use crate::exec_query::DynamoDbQueryExecutionPlan;
use crate::exec_scan::DynamoDbScanExecutionPlan;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::datasource::{Statistics, TableProvider, TableProviderFilterPushDown};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, Operator};
use datafusion::physical_plan::ExecutionPlan;
use rusoto_dynamodb::{AttributeValue, DynamoDbClient};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct DynamoDbClientWrapper(DynamoDbClient);

impl Deref for DynamoDbClientWrapper {
	type Target = DynamoDbClient;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Debug for DynamoDbClientWrapper {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DynamoDbClient")
	}
}

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
	client: Arc<DynamoDbClientWrapper>,
	def: DynamoDbTableDefinition,
}

impl DynamoDbTableProvider {
	pub fn new(client: DynamoDbClient, def: DynamoDbTableDefinition) -> Self {
		Self {
			client: Arc::new(DynamoDbClientWrapper(client)),
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
		filters: &[Expr],
		_limit: Option<usize>,
	) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
		let mut hash_key_eq = None;

		// TODO: need to handle sort key predicates here, plus range operators
		// also need to unpack conjunction/disjunction
		for filter in filters {
			// update once matching on smart ptrs is stabilised
			if let Expr::BinaryExpr { left, op, right } = filter {
				if let (Expr::Column(column_name), Operator::Eq, Expr::Literal(value)) = (&**left, op, &**right) {
					if column_name == self.def.hash_key() {
						hash_key_eq = Some(value.to_string());
					}
				}
			}
		}

		match &self.def.key {
			DynamoDbKey::Hash(hash_key) => {
				// if we're searching for an exact key on a hash-only table, use the GetItem API
				if let Some(hash_value) = hash_key_eq {
					let mut key = HashMap::new();
					key.insert(
						hash_key.clone(),
						AttributeValue {
							s: Some(hash_value),
							..Default::default()
						},
					);

					return Ok(Arc::new(DynamoDbGetItemExecutionPlan {
						client: self.client.clone(),
						def: self.def.clone(),
						key,
					}));
				}
			}
			DynamoDbKey::Composite(_, _) => {
				// if we're searching in a specific partition within a composite table, use the Query API
				if let Some(hash_value) = hash_key_eq {
					return Ok(Arc::new(DynamoDbQueryExecutionPlan {
						client: self.client.clone(),
						def: self.def.clone(),
						hash_value,
					}));
				}
			}
		}

		// safe but slow fallback: scan the table and do any necessary filtering inside DataFusion
		// TODO: allow partition tuning
		Ok(Arc::new(DynamoDbScanExecutionPlan {
			client: self.client.clone(),
			def: self.def.clone(),
			num_partitions: 1,
		}))
	}

	fn statistics(&self) -> Statistics {
		Default::default()
	}

	fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown, DataFusionError> {
		Ok(TableProviderFilterPushDown::Inexact)
	}
}
