use async_trait::async_trait;
use convergence::engine::{Engine, Portal, PreparedStatement, QueryResult};
use convergence::protocol::{DataRow, DataTypeOid, ErrorResponse, FieldDescription, FormatCode, RowDescription};
use convergence::server::{self, BindOptions};
use sqlparser::ast::Statement;
use tokio_postgres::{connect, NoTls};

struct ReturnSingleScalarPortal {
	row_desc: RowDescription,
}

#[async_trait]
impl Portal for ReturnSingleScalarPortal {
	async fn fetch(&mut self) -> Result<QueryResult, ErrorResponse> {
		Ok(QueryResult {
			rows: vec![DataRow {
				values: vec![Some(vec![0, 0, 0, 1])],
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
		statement: &PreparedStatement,
		_: FormatCode,
	) -> Result<Self::PortalType, ErrorResponse> {
		Ok(ReturnSingleScalarPortal {
			row_desc: statement.row_desc.clone(),
		})
	}
}

#[tokio::test]
async fn basic_connection() {
	let _handle = server::run_background::<ReturnSingleScalarEngine>(BindOptions::new());

	let (client, conn) = connect("postgres://localhost:5432/test", NoTls)
		.await
		.expect("failed to init client");

	let _conn_handle = tokio::spawn(async move { conn.await.unwrap() });

	let row = client.query_one("select 1", &[]).await.unwrap();
	let value: i32 = row.get(0);
	assert_eq!(value, 1);
}
