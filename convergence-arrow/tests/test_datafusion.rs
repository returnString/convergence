use async_trait::async_trait;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use datafusion::prelude::*;
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};

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
	fn new() -> Self {
		let mut ctx = ExecutionContext::new();
		ctx.register_csv("test_100_4buckets", "data/100_4buckets.csv", CsvReadOptions::new())
			.expect("failed to register csv");

		Self { ctx }
	}
}

#[async_trait]
impl Engine for DataFusionEngine {
	type PortalType = DataFusionPortal;

	async fn prepare(&mut self, statement: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		let plan = self.ctx.sql(&statement.to_string()).expect("sql failed");
		schema_to_field_desc(&plan.schema().clone().into())
	}

	async fn create_portal(&mut self, statement: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		let df = self.ctx.sql(&statement.to_string()).expect("sql failed");
		Ok(DataFusionPortal { df })
	}
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background(
		BindOptions::new().with_port(0),
		Arc::new(|| Box::pin(async { DataFusionEngine::new() })),
	)
	.await
	.unwrap();

	let (client, conn) = connect(&format!("postgres://localhost:{}/test", port), NoTls)
		.await
		.expect("failed to init client");

	tokio::spawn(async move { conn.await.unwrap() });

	client
}

#[tokio::test]
async fn count_rows() {
	let client = setup().await;

	let row = client
		.query_one("select count(*) from test_100_4buckets", &[])
		.await
		.unwrap();

	let count: i64 = row.get(0);
	assert_eq!(count, 100);
}

#[tokio::test]
async fn grouped_counts() {
	let client = setup().await;

	let rows = client
		.query(
			"select bucket, count(*) from test_100_4buckets group by bucket order by bucket",
			&[],
		)
		.await
		.unwrap();

	assert_eq!(rows.len(), 4);

	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (&str, i64) = (row.get(0), row.get(1));
		cols
	};

	assert_eq!(get_row(0), ("a", 25));
	assert_eq!(get_row(1), ("b", 25));
	assert_eq!(get_row(2), ("c", 25));
	assert_eq!(get_row(3), ("d", 25));
}
