use arrow::datatypes::DataType;
use async_trait::async_trait;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription, SqlState};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence_arrow::metadata::Catalog;
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use datafusion::catalog::catalog::MemoryCatalogProvider;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, Value};
use std::sync::Arc;

struct DataFusionPortal {
	df: Arc<dyn DataFrame>,
}

#[async_trait]
impl Portal for DataFusionPortal {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		for arrow_batch in self.df.collect().await.expect("collect failed") {
			record_batch_to_rows(&arrow_batch, batch)?;
		}
		Ok(())
	}
}

struct DataFusionEngine {
	ctx: ExecutionContext,
}

impl DataFusionEngine {
	// dummy response used for set variable statements etc
	fn dummy_response() -> Statement {
		Statement::Query(Box::new(Query {
			body: SetExpr::Select(Box::new(Select {
				projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number(
					"1".to_owned(),
					false,
				)))],
				top: None,
				sort_by: vec![],
				selection: None,
				cluster_by: vec![],
				distinct: false,
				distribute_by: vec![],
				group_by: vec![],
				from: vec![],
				having: None,
				lateral_views: vec![],
			})),
			limit: None,
			with: None,
			fetch: None,
			offset: None,
			order_by: vec![],
		}))
	}

	fn translate_statement(statement: &Statement) -> Statement {
		match statement {
			Statement::SetVariable { .. } => Self::dummy_response(),
			other => other.clone(),
		}
	}

	fn plan(&mut self, statement: &Statement) -> Result<Arc<dyn DataFrame>, ErrorResponse> {
		self.ctx
			.sql(&Self::translate_statement(statement).to_string())
			.map_err(|err| ErrorResponse::error(SqlState::FEATURE_NOT_SUPPORTED, err.to_string()))
	}
}

#[async_trait]
impl Engine for DataFusionEngine {
	type PortalType = DataFusionPortal;

	async fn new() -> Self {
		let mut ctx = ExecutionContext::with_config(
			ExecutionConfig::new()
				.with_information_schema(true)
				.create_default_catalog_and_schema(false),
		);

		let mem_catalog = Arc::new(MemoryCatalogProvider::new());
		mem_catalog.register_schema("public", Arc::new(MemorySchemaProvider::new()));

		ctx.register_catalog("datafusion", Arc::new(Catalog::new(mem_catalog)));

		ctx.register_csv(
			"test_100_4buckets",
			"convergence-arrow/data/100_4buckets.csv",
			CsvReadOptions::new(),
		)
		.expect("failed to register csv");

		ctx.register_udf(create_udf(
			"pg_backend_pid",
			vec![],
			Arc::new(DataType::Int32),
			Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(0))))),
		));

		ctx.register_udf(create_udf(
			"current_schema",
			vec![],
			Arc::new(DataType::Utf8),
			Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("public".to_owned()))))),
		));

		Self { ctx }
	}

	async fn prepare(&mut self, statement: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		let df = self.plan(statement)?;
		schema_to_field_desc(&df.schema().clone().into())
	}

	async fn create_portal(&mut self, statement: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		let df = self.plan(statement)?;
		Ok(DataFusionPortal { df })
	}
}

#[tokio::main]
async fn main() {
	server::run::<DataFusionEngine>(BindOptions::new()).await.unwrap();
}
