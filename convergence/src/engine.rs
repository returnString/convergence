use crate::protocol::{ErrorResponse, FieldDescription};
use crate::protocol_ext::DataRowBatch;
use async_trait::async_trait;
use sqlparser::ast::Statement;

#[async_trait]
pub trait Portal: Send + Sync {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse>;
}

#[async_trait]
pub trait Engine: Send + Sync {
	type PortalType: Portal;

	async fn new() -> Self;

	async fn prepare(&mut self, stmt: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse>;
	async fn create_portal(&mut self, stmt: &Statement) -> Result<Self::PortalType, ErrorResponse>;
}
