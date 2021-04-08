use arrow::array::{ArrayRef, Date32Array, Float32Array, Int32Array, StringArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use sqlparser::ast::Statement;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};

struct ArrowPortal {
	batch: RecordBatch,
}

#[async_trait]
impl Portal for ArrowPortal {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		record_batch_to_rows(&self.batch, batch)
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
		let ts_col = Arc::new(TimestampSecondArray::from_vec(
			vec![1577836800, 1580515200, 1583020800],
			None,
		)) as ArrayRef;
		let date_col = Arc::new(Date32Array::from(vec![0, 1, 2])) as ArrayRef;

		let schema = Schema::new(vec![
			Field::new("int_col", DataType::Int32, true),
			Field::new("float_col", DataType::Float32, true),
			Field::new("string_col", DataType::Utf8, true),
			Field::new("ts_col", DataType::Timestamp(TimeUnit::Second, None), true),
			Field::new("date_col", DataType::Date32, true),
		]);

		Self {
			batch: RecordBatch::try_new(Arc::new(schema), vec![int_col, float_col, string_col, ts_col, date_col])
				.expect("failed to create batch"),
		}
	}

	async fn prepare(&mut self, _: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		schema_to_field_desc(&self.batch.schema())
	}

	async fn create_portal(&mut self, _: &Statement) -> Result<Self::PortalType, ErrorResponse> {
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
async fn basic_data_types() {
	let client = setup().await;

	let rows = client.query("select 1", &[]).await.unwrap();
	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (i32, f32, &str, NaiveDateTime, NaiveDate) =
			(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
		cols
	};

	assert_eq!(
		get_row(0),
		(
			1,
			1.5,
			"a",
			NaiveDate::from_ymd(2020, 1, 1).and_hms(0, 0, 0),
			NaiveDate::from_ymd(1970, 1, 1),
		)
	);
	assert_eq!(
		get_row(1),
		(
			2,
			2.5,
			"b",
			NaiveDate::from_ymd(2020, 2, 1).and_hms(0, 0, 0),
			NaiveDate::from_ymd(1970, 1, 2)
		)
	);
	assert_eq!(
		get_row(2),
		(
			3,
			3.5,
			"c",
			NaiveDate::from_ymd(2020, 3, 1).and_hms(0, 0, 0),
			NaiveDate::from_ymd(1970, 1, 3)
		)
	);
}
