use convergence::server::{self, BindOptions};
use convergence_arrow::datafusion::DataFusionEngine;
use datafusion::prelude::*;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};

async fn new_engine() -> DataFusionEngine {
	let mut ctx = ExecutionContext::new();
	ctx.register_csv("test_100_4buckets", "data/100_4buckets.csv", CsvReadOptions::new())
		.expect("failed to register csv");

	DataFusionEngine::new(ctx)
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background(BindOptions::new().with_port(0), Arc::new(|| Box::pin(new_engine())))
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
