//! Contains the [Connection] struct, which represents an individual Postgres session, and related types.

use crate::engine::{Engine, Portal};
use crate::protocol::*;
use crate::protocol_ext::DataRowBatch;
use futures::{SinkExt, StreamExt};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

/// Describes an error that may or may not result in the termination of a connection.
#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
	/// A protocol error was encountered, e.g. an invalid message for a connection's current state.
	#[error("protocol error: {0}")]
	Protocol(#[from] ProtocolError),
	/// A Postgres error containing a SqlState code and message occurred.
	/// May result in connection termination depending on the severity.
	#[error("error response: {0}")]
	ErrorResponse(#[from] ErrorResponse),
	/// The connection was closed.
	/// This always implies connection termination.
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
	pub statement: Option<Statement>,
	pub fields: Vec<FieldDescription>,
}

struct BoundPortal<E: Engine> {
	pub portal: E::PortalType,
	pub row_desc: RowDescription,
}

/// Describes a connection using a specific engine.
/// Contains connection state including prepared statements and portals.
pub struct Connection<E: Engine> {
	engine: E,
	state: ConnectionState,
	statements: HashMap<String, PreparedStatement>,
	portals: HashMap<String, Option<BoundPortal<E>>>,
}

impl<E: Engine> Connection<E> {
	/// Create a new connection from an engine instance.
	pub fn new(engine: E) -> Self {
		Self {
			state: ConnectionState::Startup,
			statements: HashMap::new(),
			portals: HashMap::new(),
			engine,
		}
	}

	fn prepared_statement(&self, name: &str) -> Result<&PreparedStatement, ConnectionError> {
		Ok(self
			.statements
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_SQL_STATEMENT_NAME, "missing statement"))?)
	}

	fn portal(&self, name: &str) -> Result<&Option<BoundPortal<E>>, ConnectionError> {
		Ok(self
			.portals
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn portal_mut(&mut self, name: &str) -> Result<&mut Option<BoundPortal<E>>, ConnectionError> {
		Ok(self
			.portals
			.get_mut(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::INVALID_CURSOR_NAME, "missing portal"))?)
	}

	fn parse_statement(&mut self, text: &str) -> Result<Option<Statement>, ErrorResponse> {
		let statements = Parser::parse_sql(&PostgreSqlDialect {}, text)
			.map_err(|err| ErrorResponse::error(SqlState::SYNTAX_ERROR, err.to_string()))?;

		match statements.len() {
			0 => Ok(None),
			1 => Ok(Some(statements[0].clone())),
			_ => Err(ErrorResponse::error(
				SqlState::SYNTAX_ERROR,
				"expected zero or one statements",
			)),
		}
	}

	async fn step(
		&mut self,
		framed: &mut Framed<impl AsyncRead + AsyncWrite + Unpin, ConnectionCodec>,
	) -> Result<Option<ConnectionState>, ConnectionError> {
		match self.state {
			ConnectionState::Startup => {
				match framed.next().await.ok_or(ConnectionError::ConnectionClosed)?? {
					ClientMessage::Startup(_startup) => {
						// do startup stuff
					}
					_ => {
						return Err(
							ErrorResponse::fatal(SqlState::PROTOCOL_VIOLATION, "expected startup message").into(),
						)
					}
				}

				framed.send(AuthenticationOk).await?;

				let param_statuses = &[
					("server_version", "13"),
					("server_encoding", "UTF8"),
					("client_encoding", "UTF8"),
					("DateStyle", "ISO"),
					("TimeZone", "UTC"),
					("integer_datetimes", "on"),
				];

				for &(param, status) in param_statuses {
					framed.send(ParameterStatus::new(param, status)).await?;
				}

				framed.send(ReadyForQuery).await?;
				Ok(Some(ConnectionState::Idle))
			}
			ConnectionState::Idle => {
				match framed.next().await.ok_or(ConnectionError::ConnectionClosed)?? {
					ClientMessage::Parse(parse) => {
						let parsed_statement = self.parse_statement(&parse.query)?;

						self.statements.insert(
							parse.prepared_statement_name,
							PreparedStatement {
								fields: match &parsed_statement {
									Some(statement) => self.engine.prepare(statement).await?,
									None => vec![],
								},
								statement: parsed_statement,
							},
						);
						framed.send(ParseComplete).await?;
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
						let portal = match prepared.statement {
							Some(statement) => {
								let portal = self.engine.create_portal(&statement).await?;
								let row_desc = RowDescription {
									fields: prepared.fields.clone(),
									format_code,
								};

								Some(BoundPortal { portal, row_desc })
							}
							None => None,
						};

						self.portals.insert(bind.portal, portal);

						framed.send(BindComplete).await?;
					}
					ClientMessage::Describe(Describe::PreparedStatement(ref statement_name)) => {
						let fields = self.prepared_statement(statement_name)?.fields.clone();
						framed.send(ParameterDescription {}).await?;
						framed
							.send(RowDescription {
								fields,
								format_code: FormatCode::Text,
							})
							.await?;
					}
					ClientMessage::Describe(Describe::Portal(ref portal_name)) => match self.portal(portal_name)? {
						Some(portal) => framed.send(portal.row_desc.clone()).await?,
						None => framed.send(NoData).await?,
					},
					ClientMessage::Sync => {
						framed.send(ReadyForQuery).await?;
					}
					ClientMessage::Execute(exec) => match self.portal_mut(&exec.portal)? {
						Some(bound) => {
							let mut batch_writer = DataRowBatch::from_row_desc(&bound.row_desc);
							bound.portal.fetch(&mut batch_writer).await?;
							let num_rows = batch_writer.num_rows();

							framed.send(batch_writer).await?;

							framed
								.send(CommandComplete {
									command_tag: format!("SELECT {}", num_rows),
								})
								.await?;
						}
						None => {
							framed.send(EmptyQueryResponse).await?;
						}
					},
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

							framed.send(row_desc).await?;
							framed.send(batch_writer).await?;

							framed
								.send(CommandComplete {
									command_tag: format!("SELECT {}", num_rows),
								})
								.await?;
						} else {
							framed.send(EmptyQueryResponse).await?;
						}
						framed.send(ReadyForQuery).await?;
					}
					ClientMessage::Terminate => return Ok(None),
					_ => return Err(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, "unexpected message").into()),
				};

				Ok(Some(ConnectionState::Idle))
			}
		}
	}

	/// Given a stream (typically TCP), extract Postgres protocol messages and respond accordingly.
	/// This function only returns when the connection is closed (either gracefully or due to an error).
	pub async fn run(&mut self, stream: impl AsyncRead + AsyncWrite + Unpin) -> Result<(), ConnectionError> {
		let mut framed = Framed::new(stream, ConnectionCodec::new());
		loop {
			let new_state = match self.step(&mut framed).await {
				Ok(Some(state)) => state,
				Ok(None) => return Ok(()),
				Err(ConnectionError::ErrorResponse(err_info)) => {
					framed.send(err_info.clone()).await?;

					if err_info.severity == Severity::FATAL {
						return Err(err_info.into());
					}

					framed.send(ReadyForQuery).await?;
					ConnectionState::Idle
				}
				Err(err) => {
					framed
						.send(ErrorResponse::fatal(SqlState::CONNECTION_EXCEPTION, "connection error"))
						.await?;
					return Err(err);
				}
			};

			self.state = new_state;
		}
	}
}
