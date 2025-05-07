use async_trait::async_trait;
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta};
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence::sqlparser::ast::Statement;
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use datafusion::arrow::array::{
	ArrayRef, Date32Array, Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray,
	DurationNanosecondArray, DurationSecondArray, Float32Array, Int32Array, StringArray, StringViewArray,
	Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use rust_decimal::Decimal;
use std::convert::TryInto;
use std::sync::Arc;
use tokio_postgres::types::{FromSql, Type};
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
		let decimal_col = Arc::new(
			Decimal128Array::from(vec![11, 22, 33])
				.with_precision_and_scale(2, 0)
				.unwrap(),
		) as ArrayRef;
		let string_col = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
		let string_view_col = Arc::new(StringViewArray::from(vec!["aa", "bb", "cc"])) as ArrayRef;
		let ts_col = Arc::new(TimestampSecondArray::from(vec![1577836800, 1580515200, 1583020800])) as ArrayRef;
		let ts_tz_col =
			Arc::new(TimestampSecondArray::from(vec![1577854800, 1580533200, 1583038800]).with_timezone("+05:00"))
				as ArrayRef;
		let date_col = Arc::new(Date32Array::from(vec![0, 1, 2])) as ArrayRef;
		let time_s_col = Arc::new(Time32SecondArray::from(vec![30, 60, 90])) as ArrayRef;
		let time_ms_col = Arc::new(Time32MillisecondArray::from(vec![30_000, 60_000, 90_000])) as ArrayRef;
		let time_mcs_col = Arc::new(Time64MicrosecondArray::from(vec![30_000_000, 60_000_000, 90_000_000])) as ArrayRef;
		let time_ns_col = Arc::new(Time64NanosecondArray::from(vec![
			30_000_000_000,
			60_000_000_000,
			90_000_000_000,
		])) as ArrayRef;
		let duration_s_col = Arc::new(DurationSecondArray::from(vec![3, 6, 9])) as ArrayRef;
		let duration_ms_col = Arc::new(DurationMillisecondArray::from(vec![3_000, 6_000, 9_000])) as ArrayRef;
		let duration_mcs_col =
			Arc::new(DurationMicrosecondArray::from(vec![3_000_000, 6_000_000, 9_000_000])) as ArrayRef;
		let duration_ns_col = Arc::new(DurationNanosecondArray::from(vec![
			3_000_000_000,
			6_000_000_000,
			9_000_000_000,
		])) as ArrayRef;

		let schema = Schema::new(vec![
			Field::new("int_col", DataType::Int32, true),
			Field::new("float_col", DataType::Float32, true),
			Field::new("decimal_col", DataType::Decimal128(2, 0), true),
			Field::new("string_col", DataType::Utf8, true),
			Field::new("string_view_col", DataType::Utf8View, true),
			Field::new("ts_col", DataType::Timestamp(TimeUnit::Second, None), true),
			Field::new(
				"ts_tz_col",
				DataType::Timestamp(TimeUnit::Second, Some("+05:00".into())),
				true,
			),
			Field::new("date_col", DataType::Date32, true),
			Field::new("time_s_col", DataType::Time32(TimeUnit::Second), true),
			Field::new("time_ms_col", DataType::Time32(TimeUnit::Millisecond), true),
			Field::new("time_mcs_col", DataType::Time64(TimeUnit::Microsecond), true),
			Field::new("time_ns_col", DataType::Time64(TimeUnit::Nanosecond), true),
			Field::new("duration_s_col", DataType::Duration(TimeUnit::Second), true),
			Field::new("duration_ms_col", DataType::Duration(TimeUnit::Millisecond), true),
			Field::new("duration_mcs_col", DataType::Duration(TimeUnit::Microsecond), true),
			Field::new("duration_ns_col", DataType::Duration(TimeUnit::Nanosecond), true),
		]);

		Self {
			batch: RecordBatch::try_new(
				Arc::new(schema),
				vec![
					int_col,
					float_col,
					decimal_col,
					string_col,
					string_view_col,
					ts_col,
					ts_tz_col,
					date_col,
					time_s_col,
					time_ms_col,
					time_mcs_col,
					time_ns_col,
					duration_s_col,
					duration_ms_col,
					duration_mcs_col,
					duration_ns_col,
				],
			)
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

// remove after https://github.com/sfackler/rust-postgres/pull/1238 is merged
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
struct DurationWrapper(TimeDelta);

impl<'a> FromSql<'a> for DurationWrapper {
	fn from_sql(_ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		let micros = i64::from_be_bytes(raw.try_into().unwrap());
		Ok(DurationWrapper(Duration::microseconds(micros)))
	}
	fn accepts(ty: &Type) -> bool {
		matches!(ty, &Type::INTERVAL)
	}
}

#[tokio::test]
async fn basic_data_types() {
	let client = setup().await;

	let rows = client.query("select 1", &[]).await.unwrap();
	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (
			i32,
			f32,
			Decimal,
			&str,
			&str,
			NaiveDateTime,
			DateTime<FixedOffset>,
			NaiveDate,
			NaiveTime,
			NaiveTime,
			NaiveTime,
			NaiveTime,
			DurationWrapper,
			DurationWrapper,
			DurationWrapper,
			DurationWrapper,
		) = (
			row.get(0),
			row.get(1),
			row.get(2),
			row.get(3),
			row.get(4),
			row.get(5),
			row.get(6),
			row.get(7),
			row.get(8),
			row.get(9),
			row.get(10),
			row.get(11),
			row.get(12),
			row.get(13),
			row.get(14),
			row.get(15),
		);
		cols
	};

	let row = get_row(0);
	assert!(row.0 == 1);
	assert!(row.1 == 1.5);
	assert!(row.2 == Decimal::from(11));
	assert!(row.3 == "a");
	assert!(row.4 == "aa");
	assert!(
		row.5
			== NaiveDate::from_ymd_opt(2020, 1, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap()
	);
	assert!(row.6 == DateTime::from_timestamp_millis(1577854800000).unwrap());
	assert!(row.7 == NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
	assert!(row.8 == NaiveTime::from_hms_opt(0, 0, 30).unwrap());
	assert!(row.9 == NaiveTime::from_hms_opt(0, 0, 30).unwrap());
	assert!(row.10 == NaiveTime::from_hms_opt(0, 0, 30).unwrap());
	assert!(row.11 == NaiveTime::from_hms_opt(0, 0, 30).unwrap());
	assert!(row.12 == DurationWrapper(Duration::seconds(3)));
	assert!(row.13 == DurationWrapper(Duration::seconds(3)));
	assert!(row.14 == DurationWrapper(Duration::seconds(3)));
	assert!(row.15 == DurationWrapper(Duration::seconds(3)));

	let row = get_row(1);
	assert!(row.0 == 2);
	assert!(row.1 == 2.5);
	assert!(row.2 == Decimal::from(22));
	assert!(row.3 == "b");
	assert!(row.4 == "bb");
	assert!(
		row.5
			== NaiveDate::from_ymd_opt(2020, 2, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap()
	);
	assert!(row.6 == DateTime::from_timestamp_millis(1580533200000).unwrap());
	assert!(row.7 == NaiveDate::from_ymd_opt(1970, 1, 2).unwrap());
	assert!(row.8 == NaiveTime::from_hms_opt(0, 1, 0).unwrap());
	assert!(row.9 == NaiveTime::from_hms_opt(0, 1, 0).unwrap());
	assert!(row.10 == NaiveTime::from_hms_opt(0, 1, 0).unwrap());
	assert!(row.11 == NaiveTime::from_hms_opt(0, 1, 0).unwrap());
	assert!(row.12 == DurationWrapper(Duration::seconds(6)));
	assert!(row.13 == DurationWrapper(Duration::seconds(6)));
	assert!(row.14 == DurationWrapper(Duration::seconds(6)));
	assert!(row.15 == DurationWrapper(Duration::seconds(6)));

	let row = get_row(2);
	assert!(row.0 == 3);
	assert!(row.1 == 3.5);
	assert!(row.2 == Decimal::from(33));
	assert!(row.3 == "c");
	assert!(row.4 == "cc");
	assert!(
		row.5
			== NaiveDate::from_ymd_opt(2020, 3, 1)
				.unwrap()
				.and_hms_opt(0, 0, 0)
				.unwrap()
	);
	assert!(row.6 == DateTime::from_timestamp_millis(1583038800000).unwrap());
	assert!(row.7 == NaiveDate::from_ymd_opt(1970, 1, 3).unwrap());
	assert!(row.8 == NaiveTime::from_hms_opt(0, 1, 30).unwrap());
	assert!(row.9 == NaiveTime::from_hms_opt(0, 1, 30).unwrap());
	assert!(row.10 == NaiveTime::from_hms_opt(0, 1, 30).unwrap());
	assert!(row.11 == NaiveTime::from_hms_opt(0, 1, 30).unwrap());
	assert!(row.12 == DurationWrapper(Duration::seconds(9)));
	assert!(row.13 == DurationWrapper(Duration::seconds(9)));
	assert!(row.14 == DurationWrapper(Duration::seconds(9)));
	assert!(row.15 == DurationWrapper(Duration::seconds(9)));
}
