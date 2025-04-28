use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence::sqlparser::ast::Statement;
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use datafusion::arrow::array::{ArrayRef, Date32Array, Decimal128Array, Float32Array, Int32Array, StringArray, StringViewArray, TimestampSecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use rust_decimal::Decimal;
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

impl ArrowEngine {
	fn new() -> Self {
		let int_col = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
		let float_col = Arc::new(Float32Array::from(vec![1.5, 2.5, 3.5])) as ArrayRef;
		let decimal_col = Arc::new(Decimal128Array::from(vec![11, 22, 33]).with_precision_and_scale(2, 0).unwrap()) as ArrayRef;
		let string_col = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
		let string_view_col = Arc::new(StringViewArray::from(vec!["aa", "bb", "cc"])) as ArrayRef;
		let ts_col = Arc::new(TimestampSecondArray::from(vec![1577836800, 1580515200, 1583020800])) as ArrayRef;
		let date_col = Arc::new(Date32Array::from(vec![0, 1, 2])) as ArrayRef;

		let schema = Schema::new(vec![
			Field::new("int_col", DataType::Int32, true),
			Field::new("float_col", DataType::Float32, true),
			Field::new("decimal_col", DataType::Decimal128(2, 0), true),
			Field::new("string_col", DataType::Utf8, true),
			Field::new("string_view_col", DataType::Utf8View, true),
			Field::new("ts_col", DataType::Timestamp(TimeUnit::Second, None), true),
			Field::new("date_col", DataType::Date32, true),
		]);

		Self {
			batch: RecordBatch::try_new(Arc::new(schema), vec![int_col, float_col, decimal_col, string_col, string_view_col, ts_col, date_col])
				.expect("failed to create batch"),
		}
	}
}

#[async_trait]
impl Engine for ArrowEngine {
	type PortalType = ArrowPortal;

	async fn prepare(&mut self, _: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		schema_to_field_desc(&*self.batch.schema())
	}

	async fn create_portal(&mut self, _: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ArrowPortal {
			batch: self.batch.clone(),
		})
	}
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background(
		BindOptions::new().with_port(0),
		Arc::new(|| Box::pin(async { ArrowEngine::new() })),
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
async fn basic_data_types() {
	let client = setup().await;

	let rows = client.query("select 1", &[]).await.unwrap();
	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (i32, f32, Decimal, &str, &str, NaiveDateTime, NaiveDate) =
			(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6));
		cols
	};

	assert_eq!(
		get_row(0),
		(
			1,
			1.5,
			Decimal::from(11),
			"a",
			"aa",
			NaiveDate::from_ymd_opt(2020, 1, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap(),
			NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
		)
	);
	assert_eq!(
		get_row(1),
		(
			2,
			2.5,
			Decimal::from(22),
			"b",
			"bb",
			NaiveDate::from_ymd_opt(2020, 2, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap(),
			NaiveDate::from_ymd_opt(1970, 1, 2).unwrap()
		)
	);
	assert_eq!(
		get_row(2),
		(
			3,
			3.5,
			Decimal::from(33),
			"c",
			"cc",
			NaiveDate::from_ymd_opt(2020, 3, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap(),
			NaiveDate::from_ymd_opt(1970, 1, 3).unwrap()
		)
	);
}
