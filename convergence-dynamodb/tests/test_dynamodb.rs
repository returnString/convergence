use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use convergence_dynamodb::provider::{DynamoDbKey, DynamoDbTableDefinition, DynamoDbTableProvider};
use datafusion::prelude::*;
use rusoto_core::{credential::StaticProvider, Client, HttpClient, Region};
use rusoto_dynamodb::{
	AttributeDefinition, AttributeValue, CreateTableInput, DynamoDb, DynamoDbClient, KeySchemaElement, PutItemInput,
};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::{connect, NoTls};
use uuid::Uuid;

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

#[async_trait]
impl Engine for DataFusionEngine {
	type PortalType = DataFusionPortal;

	async fn new() -> Self {
		let ddb_table_name = Uuid::new_v4().to_simple().to_string();

		// use the extended client init to avoid issues in rusoto's usage of hyper
		// https://github.com/hyperium/hyper/issues/2112
		let ddb_client = DynamoDbClient::new_with_client(
			Client::new_with(
				StaticProvider::new("blah".to_owned(), "blah".to_owned(), None, None),
				HttpClient::new().unwrap(),
			),
			Region::Custom {
				name: "test".to_owned(),
				endpoint: "http://localhost:8000".to_owned(),
			},
		);

		ddb_client
			.create_table(CreateTableInput {
				table_name: ddb_table_name.clone(),
				attribute_definitions: vec![AttributeDefinition {
					attribute_name: "some_id".to_owned(),
					attribute_type: "S".to_owned(),
				}],
				key_schema: vec![KeySchemaElement {
					attribute_name: "some_id".to_owned(),
					key_type: "HASH".to_owned(),
				}],
				billing_mode: Some("PAY_PER_REQUEST".to_owned()),
				..Default::default()
			})
			.await
			.expect("failed to create ddb table");

		for i in 0..10 {
			let mut item = HashMap::new();
			item.insert(
				"some_id".to_owned(),
				AttributeValue {
					s: Some(format!("item_{}", i)),
					..Default::default()
				},
			);
			item.insert(
				"float_val".to_owned(),
				AttributeValue {
					n: Some(format!("{}", (i as f64) * 1.5)),
					..Default::default()
				},
			);

			ddb_client
				.put_item(PutItemInput {
					table_name: ddb_table_name.clone(),
					item,
					..Default::default()
				})
				.await
				.expect("failed to put item");
		}

		let table_schema = Schema::new(vec![
			Field::new("some_id", DataType::Utf8, true),
			Field::new("float_val", DataType::Float64, true),
		]);

		let mut ctx = ExecutionContext::new();
		ctx.register_table(
			"ddb_test",
			Arc::new(DynamoDbTableProvider::new(
				ddb_client,
				DynamoDbTableDefinition::new(
					ddb_table_name,
					DynamoDbKey::Hash("some_id".to_owned()),
					Arc::new(table_schema),
				),
			)),
		)
		.expect("failed to register table");

		Self { ctx }
	}

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

	let row = client.query_one("select count(*) from ddb_test", &[]).await.unwrap();

	let count: i64 = row.get(0);
	assert_eq!(count, 10);
}

#[tokio::test]
async fn row_values() {
	let client = setup().await;

	let rows = client
		.query("select some_id, float_val from ddb_test order by some_id", &[])
		.await
		.unwrap();

	assert_eq!(rows.len(), 10);

	let get_row = |idx: usize| {
		let row = &rows[idx];
		let cols: (&str, f64) = (row.get(0), row.get(1));
		cols
	};

	for i in 0..10 {
		assert_eq!(get_row(i), (format!("item_{}", i).as_str(), (i as f64) * 1.5));
	}
}
