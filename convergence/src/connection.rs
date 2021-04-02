use crate::engine::{Engine, Portal, PreparedStatement};
use crate::protocol::*;
use futures::{SinkExt, StreamExt};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
	#[error("io error: {0}")]
	Io(#[from] std::io::Error),
	#[error("protocol error: {0}")]
	Protocol(#[from] ProtocolError),
	#[error("parser error: {0}")]
	Parser(#[from] ParserError),
	#[error("error response: {0}")]
	ErrorResponse(#[from] ErrorResponse),
	#[error("connection closed")]
	ConnectionClosed,
}

#[derive(Debug)]
enum ConnectionState {
	Startup,
	Idle,
}

pub struct Connection<E: Engine, S> {
	engine: E,
	framed: Framed<S, ConnectionCodec>,
	state: ConnectionState,
	statements: HashMap<String, PreparedStatement>,
	portals: HashMap<String, E::PortalType>,
}

impl<E: Engine, S: AsyncRead + AsyncWrite + Unpin> Connection<E, S> {
	pub fn new(stream: S, engine: E) -> Self {
		Self {
			framed: Framed::new(stream, ConnectionCodec::new()),
			state: ConnectionState::Startup,
			statements: HashMap::new(),
			portals: HashMap::new(),
			engine,
		}
	}

	async fn send(&mut self, message: impl BackendMessage) -> Result<(), ConnectionError> {
		self.framed.send(message).await?;
		Ok(())
	}

	async fn next(&mut self) -> Result<ClientMessage, ConnectionError> {
		Ok(self.framed.next().await.ok_or(ConnectionError::ConnectionClosed)??)
	}

	fn prepared_statement(&self, name: &str) -> Result<&PreparedStatement, ConnectionError> {
		Ok(self
			.statements
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_SQL_STATEMENT_NAME, "missing statement"))?)
	}

	fn portal(&self, name: &str) -> Result<&E::PortalType, ConnectionError> {
		Ok(self
			.portals
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn portal_mut(&mut self, name: &str) -> Result<&mut E::PortalType, ConnectionError> {
		Ok(self
			.portals
			.get_mut(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn parse_statement(text: &str) -> Result<Statement, ConnectionError> {
		let statements = Parser::parse_sql(&PostgreSqlDialect {}, text)?;
		if statements.len() != 1 {
			Err(ErrorResponse::error(SqlState::SYNTAX_ERROR, "expected exactly one statement").into())
		} else {
			Ok(statements[0].clone())
		}
	}

	async fn step(&mut self) -> Result<ConnectionState, ConnectionError> {
		match self.state {
			ConnectionState::Startup => {
				match self.next().await? {
					ClientMessage::Startup(_startup) => {
						// do startup stuff
					}
					_ => {
						return Err(
							ErrorResponse::fatal(SqlState::PROTOCOL_VIOLATION, "expected startup message").into(),
						)
					}
				}

				self.send(AuthenticationOk).await?;
				self.send(ReadyForQuery).await?;
				Ok(ConnectionState::Idle)
			}
			ConnectionState::Idle => {
				match self.next().await? {
					ClientMessage::Parse(parse) => {
						let statement = Self::parse_statement(&parse.query)?;
						self.statements
							.insert(parse.prepared_statement_name, self.engine.prepare(statement).await?);
						self.send(ParseComplete).await?;
					}
					ClientMessage::Bind(bind) => {
						let format_code = match bind.result_format {
							BindFormat::All(format) => format,
							BindFormat::PerColumn(_) => {
								return Err(ErrorResponse::error(
									SqlState::FEATURE_NOT_SUPPORTED,
									"per-column format codes not supported",
								)
								.into());
							}
						};

						let statement = self.prepared_statement(&bind.prepared_statement_name)?.clone();
						let portal = self.engine.create_portal(statement, format_code).await?;

						self.portals.insert(bind.portal, portal);
						self.send(BindComplete).await?;
					}
					ClientMessage::Describe(Describe::PreparedStatement(ref statement_name)) => {
						let row_desc = self.prepared_statement(statement_name)?.row_desc.clone();
						self.send(ParameterDescription {}).await?;
						self.send(row_desc).await?;
					}
					ClientMessage::Describe(Describe::Portal(ref portal_name)) => {
						let row_desc = self.portal(portal_name)?.row_desc();
						self.send(row_desc).await?;
					}
					ClientMessage::Sync => {
						self.send(ReadyForQuery).await?;
					}
					ClientMessage::Execute(exec) => {
						let portal = self.portal_mut(&exec.portal)?;
						let result = portal.fetch().await?;
						let num_rows = result.rows.len();

						for row in result.rows {
							self.send(row).await?;
						}

						self.send(CommandComplete {
							command_tag: format!("SELECT {}", num_rows),
						})
						.await?;
					}
					ClientMessage::Query(query) => {
						let parsed = Self::parse_statement(&query)?;
						let statement = self.engine.prepare(parsed).await?;
						let mut portal = self.engine.create_portal(statement, FormatCode::Text).await?;

						let result = portal.fetch().await?;
						let num_rows = result.rows.len();

						self.send(portal.row_desc()).await?;

						for row in result.rows {
							self.send(row).await?;
						}

						self.send(CommandComplete {
							command_tag: format!("SELECT {}", num_rows),
						})
						.await?;

						self.send(ReadyForQuery).await?;
					}
					_ => return Err(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, "unexpected message").into()),
				};

				Ok(ConnectionState::Idle)
			}
		}
	}

	pub async fn run(&mut self) -> Result<(), ConnectionError> {
		loop {
			let new_state = match self.step().await {
				Ok(state) => state,
				Err(ConnectionError::ErrorResponse(err_info)) => {
					self.send(err_info.clone()).await?;

					if err_info.severity == Severity::FATAL {
						return Err(err_info.into());
					}

					ConnectionState::Idle
				}
				Err(err) => {
					self.send(ErrorResponse::fatal(SqlState::CONNECTION_EXCEPTION, "connection error"))
						.await?;
					return Err(err);
				}
			};

			self.state = new_state;
		}
	}
}
