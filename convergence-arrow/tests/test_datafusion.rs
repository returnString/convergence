use async_trait::async_trait;
use convergence::engine::{Engine, Portal, PreparedStatement, QueryResult};
use convergence::protocol::{ErrorResponse, FormatCode, RowDescription};
use convergence::server::{self, BindOptions};
use convergence_arrow::table::{record_batch_to_rows, schema_to_row_desc};
use datafusion::prelude::*;
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};

struct DataFusionPortal {
	df: Arc<dyn DataFrame>,
	format_code: FormatCode,
}

#[async_trait]
impl Portal for DataFusionPortal {
	fn row_desc(&self) -> RowDescription {
		schema_to_row_desc(&self.df.schema().clone().into(), self.format_code)
	}

	async fn fetch(&mut self) -> Result<QueryResult, ErrorResponse> {
		let mut rows = Vec::new();
		for batch in self.df.collect().await.expect("collect failed") {
			rows.extend(record_batch_to_rows(&batch, self.format_code));
		}

		Ok(QueryResult { rows })
	}
}

struct DataFusionEngine {
	ctx: ExecutionContext,
}

#[async_trait]
impl Engine for DataFusionEngine {
	type PortalType = DataFusionPortal;

	async fn new() -> Self {
		let mut ctx = ExecutionContext::new();
		ctx.register_csv("test_100_4buckets", "data/100_4buckets.csv", CsvReadOptions::new())
			.expect("failed to register csv");

		Self { ctx }
	}

	async fn prepare(&mut self, statement: Statement) -> Result<PreparedStatement, ErrorResponse> {
		let plan = self.ctx.sql(&statement.to_string()).expect("sql failed");

		Ok(PreparedStatement {
			statement,
			row_desc: schema_to_row_desc(&plan.schema().clone().into(), FormatCode::Text),
		})
	}

	async fn create_portal(
		&mut self,
		statement: PreparedStatement,
		format_code: FormatCode,
	) -> Result<Self::PortalType, ErrorResponse> {
		let df = self.ctx.sql(&statement.statement.to_string()).expect("sql failed");
		Ok(DataFusionPortal { df, format_code })
	}
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background::<DataFusionEngine>(BindOptions::new().with_port(0))
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
