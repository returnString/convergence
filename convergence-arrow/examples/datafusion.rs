use convergence::server::{self, BindOptions};
use convergence_arrow::datafusion::DataFusionEngine;
use convergence_arrow::metadata::Catalog;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::catalog::MemoryCatalogProvider;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

async fn new_engine() -> DataFusionEngine {
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

	DataFusionEngine::new(ctx)
}

#[tokio::main]
async fn main() {
	server::run(BindOptions::new(), Arc::new(|| Box::pin(new_engine())))
		.await
		.unwrap();
}
