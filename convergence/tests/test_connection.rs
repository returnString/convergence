use async_trait::async_trait;
use bytes::Bytes;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{DataTypeOid, ErrorResponse, FieldDescription, SqlState, StatementDescription};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use std::sync::Arc;
use tokio_postgres::{connect, NoTls, SimpleQueryMessage};

struct ReturnSingleScalarPortal;

#[async_trait]
impl Portal for ReturnSingleScalarPortal {
	async fn execute(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		let mut row = batch.create_row();
		row.write_int4(1);
		Ok(())
	}
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<Vec<FieldDescription>, ErrorResponse> {
		self.fetch(batch).await?;

		Ok(vec![FieldDescription {
			name: "test".to_owned(),
			data_type: DataTypeOid::Int4,
		}])
	}
}

struct ReturnSingleScalarEngine;

#[async_trait]
impl Engine for ReturnSingleScalarEngine {
	type PortalType = ReturnSingleScalarPortal;

	async fn prepare(&mut self, statement: &Statement) -> Result<StatementDescription, ErrorResponse> {
		if let Statement::Query(query) = &statement {
			if let SetExpr::Select(select) = &*query.body {
				if select.projection.len() == 1 {
					if let SelectItem::UnnamedExpr(Expr::Identifier(column_name)) = &select.projection[0] {
						match column_name.value.as_str() {
							"test_error" => return Err(ErrorResponse::error(SqlState::DataException, "test error")),
							"test_fatal" => return Err(ErrorResponse::fatal(SqlState::DataException, "fatal error")),
							_ => (),
						}
					}
				}
			}
		}

		let fields = vec![FieldDescription {
			name: "test".to_owned(),
			data_type: DataTypeOid::Int4,
		}];

		Ok(StatementDescription {
			fields: Some(fields),
			parameters: None
		})

	}

	async fn create_portal(&mut self, _: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ReturnSingleScalarPortal)
	}

	async fn create_and_bind_portal(&mut self, _statement: &Statement, _params: Vec<DataTypeOid>, _binding: Vec<Bytes>) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ReturnSingleScalarPortal)
	}

}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background(
		BindOptions::new().with_port(0),
		Arc::new(|| Box::pin(async { ReturnSingleScalarEngine })),
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
async fn extended_query_flow() {
	let client = setup().await;
	let row = client.query_one("select 1", &[]).await.unwrap();
	let value: i32 = row.get(0);
	assert_eq!(value, 1);
}

#[tokio::test]
async fn simple_query_flow() {
	let client = setup().await;
	let messages = client.simple_query("select 1").await.unwrap();
	assert_eq!(messages.len(), 2);

	let row = match &messages[0] {
		SimpleQueryMessage::Row(row) => row,
		_ => panic!("expected row"),
	};

	assert_eq!(row.get(0), Some("1"));

	let num_rows = match &messages[1] {
		SimpleQueryMessage::CommandComplete(rows) => *rows,
		_ => panic!("expected command complete"),
	};

	assert_eq!(num_rows, 1);
}

#[tokio::test]
async fn error_handling() {
	let client = setup().await;
	let err = client
		.query_one("select test_error from blah", &[])
		.await
		.expect_err("expected error in query");

	assert_eq!(err.code().unwrap().code(), SqlState::DataException.code());
}

#[tokio::test]
async fn set_variable_noop() {
	let client = setup().await;
	client
		.simple_query("set somevar to 'my_val'")
		.await
		.expect("failed to set var");
}

#[tokio::test]
async fn empty_simple_query() {
	let client = setup().await;
	client.simple_query("").await.unwrap();
}

#[tokio::test]
async fn empty_extended_query() {
	let client = setup().await;
	client.query("", &[]).await.unwrap();
}
