use crate::protocol::{ErrorResponse, FormatCode, RowDescription};
use crate::protocol_ext::DataRowBatch;
use async_trait::async_trait;
use sqlparser::ast::Statement;

#[derive(Debug, Clone)]
pub struct PreparedStatement {
	pub statement: Statement,
	pub row_desc: RowDescription,
}

#[async_trait]
pub trait Portal: Send + Sync {
	fn row_desc(&self) -> RowDescription;

	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse>;
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
