use crate::engine::{Engine, Portal};
use crate::protocol::*;
use crate::protocol_ext::DataRowBatch;
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

#[derive(Debug, Clone)]
struct PreparedStatement {
	pub statement: Statement,
	pub fields: Vec<FieldDescription>,
}

struct BoundPortal<E: Engine> {
	pub portal: E::PortalType,
	pub row_desc: RowDescription,
}

pub struct Connection<E: Engine, S> {
	engine: E,
	framed: Framed<S, ConnectionCodec>,
	state: ConnectionState,
	statements: HashMap<String, PreparedStatement>,
	portals: HashMap<String, BoundPortal<E>>,
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

	async fn next(&mut self) -> Result<ClientMessage, ConnectionError> {
		Ok(self.framed.next().await.ok_or(ConnectionError::ConnectionClosed)??)
	}

	fn prepared_statement(&self, name: &str) -> Result<&PreparedStatement, ConnectionError> {
		Ok(self
			.statements
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_SQL_STATEMENT_NAME, "missing statement"))?)
	}

	fn portal(&self, name: &str) -> Result<&BoundPortal<E>, ConnectionError> {
		Ok(self
			.portals
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn portal_mut(&mut self, name: &str) -> Result<&mut BoundPortal<E>, ConnectionError> {
		Ok(self
			.portals
			.get_mut(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn parse_statement(&mut self, text: &str) -> Result<Option<Statement>, ConnectionError> {
		let statements = Parser::parse_sql(&PostgreSqlDialect {}, text)?;
		if statements.len() != 1 {
			Err(ErrorResponse::error(SqlState::SYNTAX_ERROR, "expected exactly one statement").into())
		} else {
			let statement = &statements[0];
			Ok(match statement {
				Statement::SetVariable { .. } => None,
				other => Some(other.clone()),
			})
		}
	}

	async fn step(&mut self) -> Result<Option<ConnectionState>, ConnectionError> {
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

				self.framed.send(AuthenticationOk).await?;

				let param_statuses = &[
					("server_version", "13"),
					("server_encoding", "UTF8"),
					("client_encoding", "UTF8"),
					("DateStyle", "ISO"),
					("TimeZone", "UTC"),
					("integer_datetimes", "on"),
				];

				for &(param, status) in param_statuses {
					self.framed.send(ParameterStatus::new(param, status)).await?;
				}

				self.framed.send(ReadyForQuery).await?;
				Ok(Some(ConnectionState::Idle))
			}
			ConnectionState::Idle => {
				match self.next().await? {
					ClientMessage::Parse(parse) => {
						let statement = self.parse_statement(&parse.query)?.ok_or_else(|| {
							ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, "invalid statement for parsing")
						})?;
						self.statements.insert(
							parse.prepared_statement_name,
							PreparedStatement {
								fields: self.engine.prepare(&statement).await?,
								statement,
							},
						);
						self.framed.send(ParseComplete).await?;
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

						let prepared = self.prepared_statement(&bind.prepared_statement_name)?.clone();
						let portal = self.engine.create_portal(&prepared.statement).await?;
						let row_desc = RowDescription {
							fields: prepared.fields.clone(),
							format_code,
						};

						self.portals.insert(bind.portal, BoundPortal { portal, row_desc });
						self.framed.send(BindComplete).await?;
					}
					ClientMessage::Describe(Describe::PreparedStatement(ref statement_name)) => {
						let fields = self.prepared_statement(statement_name)?.fields.clone();
						self.framed.send(ParameterDescription {}).await?;
						self.framed
							.send(RowDescription {
								fields,
								format_code: FormatCode::Text,
							})
							.await?;
					}
					ClientMessage::Describe(Describe::Portal(ref portal_name)) => {
						let row_desc = self.portal(portal_name)?.row_desc.clone();
						self.framed.send(row_desc).await?;
					}
					ClientMessage::Sync => {
						self.framed.send(ReadyForQuery).await?;
					}
					ClientMessage::Execute(exec) => {
						let bound = self.portal_mut(&exec.portal)?;

						let mut batch_writer = DataRowBatch::from_row_desc(&bound.row_desc);
						bound.portal.fetch(&mut batch_writer).await?;
						let num_rows = batch_writer.num_rows();

						self.framed.send(batch_writer).await?;

						self.framed
							.send(CommandComplete {
								command_tag: format!("SELECT {}", num_rows),
							})
							.await?;
					}
					ClientMessage::Query(query) => {
						if let Some(parsed) = self.parse_statement(&query)? {
							let fields = self.engine.prepare(&parsed).await?;
							let row_desc = RowDescription {
								fields,
								format_code: FormatCode::Text,
							};
							let mut portal = self.engine.create_portal(&parsed).await?;

							let mut batch_writer = DataRowBatch::from_row_desc(&row_desc);
							portal.fetch(&mut batch_writer).await?;
							let num_rows = batch_writer.num_rows();

							self.framed.send(row_desc).await?;
							self.framed.send(batch_writer).await?;

							self.framed
								.send(CommandComplete {
									command_tag: format!("SELECT {}", num_rows),
								})
								.await?;
						}
						self.framed.send(ReadyForQuery).await?;
					}
					ClientMessage::Terminate => return Ok(None),
					_ => return Err(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, "unexpected message").into()),
				};

				Ok(Some(ConnectionState::Idle))
			}
		}
	}

	pub async fn run(&mut self) -> Result<(), ConnectionError> {
		loop {
			let new_state = match self.step().await {
				Ok(Some(state)) => state,
				Ok(None) => return Ok(()),
				Err(ConnectionError::ErrorResponse(err_info)) => {
					self.framed.send(err_info.clone()).await?;

					if err_info.severity == Severity::FATAL {
						return Err(err_info.into());
					}

					ConnectionState::Idle
				}
				Err(err) => {
					self.framed
						.send(ErrorResponse::fatal(SqlState::CONNECTION_EXCEPTION, "connection error"))
						.await?;
					return Err(err);
				}
			};

			self.state = new_state;
		}
	}
}
