//! Provides a DataFusion-powered implementation of the [Engine] trait.

use crate::table::{record_batch_to_rows, schema_to_field_desc};
use async_trait::async_trait;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription, SqlState};
use convergence::protocol_ext::DataRowBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, Value};

fn df_err_to_sql(err: DataFusionError) -> ErrorResponse {
	ErrorResponse::error(SqlState::DataException, err.to_string())
}

// dummy query used as replacement for set variable statements etc
fn dummy_query() -> Statement {
	Statement::Query(Box::new(Query {
		body: Box::new(SetExpr::Select(Box::new(Select {
			into: None,
			qualify: None,
			projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number(
				"1".to_owned(),
				false,
			)))],
			top: None,
			sort_by: vec![],
			selection: None,
			cluster_by: vec![],
			distinct: None,
			distribute_by: vec![],
			group_by: vec![],
			from: vec![],
			having: None,
			lateral_views: vec![],
			named_window: vec![],
		}))),
		locks: vec![],
		limit: None,
		with: None,
		fetch: None,
		offset: None,
		order_by: vec![],
	}))
}

fn translate_statement(statement: &Statement) -> Statement {
	match statement {
		Statement::SetVariable { .. } => dummy_query(),
		other => other.clone(),
	}
}

/// A portal built using a logical DataFusion query plan.
pub struct DataFusionPortal {
	df: DataFrame,
}

#[async_trait]
impl Portal for DataFusionPortal {
	async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
		for arrow_batch in self.df.clone().collect().await.map_err(df_err_to_sql)? {
			record_batch_to_rows(&arrow_batch, batch)?;
		}
		Ok(())
	}
}

/// An engine instance using DataFusion for catalogue management and queries.
pub struct DataFusionEngine {
	ctx: SessionContext,
}

impl DataFusionEngine {
	/// Creates a new engine instance using the given DataFusion execution context.
	pub fn new(ctx: SessionContext) -> Self {
		Self { ctx }
	}
}

#[async_trait]
impl Engine for DataFusionEngine {
	type PortalType = DataFusionPortal;

	async fn prepare(&mut self, statement: &Statement) -> Result<Vec<FieldDescription>, ErrorResponse> {
		let plan = self
			.ctx
			.sql(&translate_statement(statement).to_string())
			.await
			.map_err(df_err_to_sql)?;

		schema_to_field_desc(&plan.schema().clone().into())
	}

	async fn create_portal(&mut self, statement: &Statement) -> Result<Self::PortalType, ErrorResponse> {
		let df = self
			.ctx
			.sql(&translate_statement(statement).to_string())
			.await
			.map_err(df_err_to_sql)?;

		Ok(DataFusionPortal { df })
	}
}
