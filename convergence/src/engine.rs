use crate::protocol::{DataRow, ErrorResponse, FormatCode, RowDescription};
use async_trait::async_trait;
use sqlparser::ast::Statement;

pub struct QueryResult {
	pub rows: Vec<DataRow>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
	pub statement: Statement,
	pub row_desc: RowDescription,
}

#[async_trait]
pub trait Portal: Send + Sync {
	fn row_desc(&self) -> RowDescription;

	async fn fetch(&mut self) -> Result<QueryResult, ErrorResponse>;
}

#[async_trait]
pub trait Engine: Send + Sync {
	type PortalType: Portal;

	async fn new() -> Self;

	async fn prepare(&mut self, stmt: Statement) -> Result<PreparedStatement, ErrorResponse>;
	async fn create_portal(
		&mut self,
		stmt: PreparedStatement,
		format_code: FormatCode,
	) -> Result<Self::PortalType, ErrorResponse>;
}
