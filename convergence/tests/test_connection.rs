use async_trait::async_trait;
use convergence::engine::{Engine, Portal, PreparedStatement, QueryResult};
use convergence::protocol::{
	DataRow, DataTypeOid, ErrorResponse, FieldDescription, FormatCode, RowDescription, SqlState,
};
use convergence::server::{self, BindOptions};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use tokio_postgres::{connect, NoTls, SimpleQueryMessage};

struct ReturnSingleScalarPortal {
	row_desc: RowDescription,
}

#[async_trait]
impl Portal for ReturnSingleScalarPortal {
	async fn fetch(&mut self) -> Result<QueryResult, ErrorResponse> {
		Ok(QueryResult {
			rows: vec![DataRow {
				values: match self.row_desc.format_code {
					FormatCode::Binary => vec![Some(vec![0, 0, 0, 1])],
					FormatCode::Text => vec![Some("1".as_bytes().to_vec())],
				},
			}],
		})
	}

	fn row_desc(&self) -> RowDescription {
		self.row_desc.clone()
	}
}

struct ReturnSingleScalarEngine;

#[async_trait]
impl Engine for ReturnSingleScalarEngine {
	type PortalType = ReturnSingleScalarPortal;

	async fn new() -> Self {
		Self
	}

	async fn prepare(&mut self, statement: Statement) -> Result<PreparedStatement, ErrorResponse> {
		if let Statement::Query(query) = &statement {
			if let SetExpr::Select(select) = &query.body {
				if select.projection.len() == 1 {
					if let SelectItem::UnnamedExpr(Expr::Identifier(column_name)) = &select.projection[0] {
						match column_name.value.as_str() {
							"test_error" => return Err(ErrorResponse::error(SqlState::DATA_EXCEPTION, "test error")),
							"test_fatal" => return Err(ErrorResponse::fatal(SqlState::DATA_EXCEPTION, "fatal error")),
							_ => (),
						}
					}
				}
			}
		}

		Ok(PreparedStatement {
			statement,
			row_desc: RowDescription {
				format_code: FormatCode::Text,
				fields: vec![FieldDescription {
					name: "test".to_owned(),
					data_type: DataTypeOid::Int4,
				}],
			},
		})
	}

	async fn create_portal(
		&mut self,
		mut statement: PreparedStatement,
		format_code: FormatCode,
	) -> Result<Self::PortalType, ErrorResponse> {
		statement.row_desc.format_code = format_code;
		Ok(ReturnSingleScalarPortal {
			row_desc: statement.row_desc,
		})
	}
}

async fn setup() -> tokio_postgres::Client {
	let port = server::run_background::<ReturnSingleScalarEngine>(BindOptions::new().with_port(0))
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

	assert_eq!(err.code().unwrap().code(), SqlState::DATA_EXCEPTION.0);
}
