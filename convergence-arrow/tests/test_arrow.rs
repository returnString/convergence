use arrow::array::{ArrayRef, Float32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use convergence::engine::{Engine, Portal, PreparedStatement};
use convergence::protocol::{ErrorResponse, FormatCode, RowDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence_arrow::table::{record_batch_to_rows, schema_to_row_desc};
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};

struct ArrowPortal {
	batch: RecordBatch,
}

#[async_trait]
impl Portal for ArrowPortal {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		record_batch_to_rows(&self.batch, batch);
		Ok(())
	}

	fn row_desc(&self) -> RowDescription {
		schema_to_row_desc(&self.batch.schema(), FormatCode::Binary)
	}
}

struct ArrowEngine {
	batch: RecordBatch,
}

#[async_trait]
impl Engine for ArrowEngine {
	type PortalType = ArrowPortal;

	async fn new() -> Self {
		let int_col = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
		let float_col = Arc::new(Float32Array::from(vec![1.5, 2.5, 3.5])) as ArrayRef;
		let string_col = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;

		let schema = Schema::new(vec![
			Field::new("int_col", DataType::Int32, true),
			Field::new("float_col", DataType::Float32, true),
			Field::new("string_col", DataType::Utf8, true),
		]);

		Self {
			batch: RecordBatch::try_new(Arc::new(schema), vec![int_col, float_col, string_col])
				.expect("failed to create batch"),
		}
	}

	async fn prepare(&mut self, statement: Statement) -> Result<PreparedStatement, ErrorResponse> {
		Ok(PreparedStatement {
			statement,
			row_desc: schema_to_row_desc(&self.batch.schema(), FormatCode::Text),
		})
	}

	async fn create_portal(&mut self, _: PreparedStatement, _: FormatCode) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ArrowPortal {
			batch: self.batch.clone(),
		})
	}
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background::<ArrowEngine>(BindOptions::new().with_port(0))
		.await
		.unwrap();

	let (client, conn) = connect(&format!("postgres://localhost:{}/test", port), NoTls)
		.await
		.expect("failed to init client");

	tokio::spawn(async move { conn.await.unwrap() });

	client
}

#[tokio::test]
async fn basic_connection() {
	let client = setup().await;

	let rows = client.query("select 1", &[]).await.unwrap();
	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (i32, f32, &str) = (row.get(0), row.get(1), row.get(2));
		cols
	};

	assert_eq!(get_row(0), (1, 1.5, "a"));
	assert_eq!(get_row(1), (2, 2.5, "b"));
	assert_eq!(get_row(2), (3, 3.5, "c"));
}
