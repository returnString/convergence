//! Contains the [Connection] struct, which represents an individual Postgres session, and related types.

use crate::engine::{Engine, Portal};
use crate::protocol::*;
use crate::protocol_ext::DataRowBatch;
use futures::{SinkExt, StreamExt};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
// use std::fmt;
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
/// Wraps Parsed Statement and associated metadata
pub struct PreparedStatement {
	pub statement: String,
	pub fields: Vec<FieldDescription>,
	pub parameters: Vec<DataTypeOid>,
}

#[derive(Debug)]
struct BoundPortal<E: Engine> {
	pub portal: E::PortalType,
	pub row_desc: RowDescription,
}

/// Describes a connection using a specific engine.
/// Contains connection state including prepared statements and portals.
pub struct Connection<E: Engine> {
	id: String,
	engine: E,
	state: ConnectionState,
	statements: HashMap<String, PreparedStatement>,
	portals: HashMap<String, BoundPortal<E>>,
}

fn id() -> String {
	use rand::distributions::{Alphanumeric, DistString};
	Alphanumeric.sample_string(&mut rand::thread_rng(), 24).to_lowercase()
}

impl<E: Engine> Connection<E> {
	/// Create a new connection from an engine instance.
	pub fn new(engine: E) -> Self {
		Self {
			id: id(),
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
			.ok_or_else(|| ErrorResponse::error(SqlState::InvalidSQLStatementName, "missing statement"))?)
	}

	fn portal(&self, name: &str) -> Result<&BoundPortal<E>, ConnectionError> {
		Ok(self
			.portals
			.get(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::InvalidCursorName, "missing portal"))?)
	}

	fn portal_mut(&mut self, name: &str) -> Result<&mut BoundPortal<E>, ConnectionError> {
		Ok(self
			.portals
			.get_mut(name)
			.ok_or_else(|| ErrorResponse::error(SqlState::InvalidCursorName, "missing portal"))?)
	}

	fn parse_statement(&mut self, text: &str) -> Result<Option<Statement>, ErrorResponse> {
		let statements = Parser::parse_sql(&PostgreSqlDialect {}, text)
			.map_err(|err| ErrorResponse::error(SqlState::SyntaxError, err.to_string()))?;

		match statements.len() {
			0 => Ok(None),
			1 => Ok(Some(statements[0].clone())),
			_ => Err(ErrorResponse::error(
				SqlState::SyntaxError,
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
						// tracing::debug!("startup {:?}", startup);
					}
					ClientMessage::SSLRequest => {
						// we don't support SSL for now
						// client will retry with startup packet
						framed.send(SSLResponse(false)).await?;
						return Ok(Some(ConnectionState::Startup));
					}
					_ => {
						return Err(
							ErrorResponse::fatal(SqlState::ProtocolViolation, "expected startup message").into(),
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
						tracing::debug!("Connection.Parse {}", self.id);

						tracing::debug!("Preparing Statement Engine {}", self.id);

						let statement_description = self
							.engine
							.prepare(&parse.prepared_statement_name, &parse.query, parse.parameter_types)
							.await?;

						let prepared_statement = PreparedStatement {
							statement: parse.query.to_owned(),
							parameters: statement_description.parameters.unwrap_or(vec![]),
							fields: statement_description.fields.unwrap_or(vec![]),
						};

						self.statements
							.insert(parse.prepared_statement_name, prepared_statement);

						framed.send(ParseComplete).await?;
					}
					ClientMessage::Bind(bind) => {
						tracing::debug!("Connection.Bind {}", self.id);

						let result_format = match bind.result_format {
							BindFormat::All(format) => format,
							BindFormat::PerColumn(_) => {
								return Err(ErrorResponse::error(
									SqlState::FeatureNotSupported,
									"per-column format codes not supported",
								)
								.into());
							}
						};

						let prepared = self.prepared_statement(&bind.prepared_statement_name);

						match prepared {
							Ok(prepared) => {
								tracing::debug!("Connection.Bind {} Prepared={}", self.id, &prepared.statement);

								let params = prepared.parameters.to_owned();
								let binding = bind.parameters.to_owned();

								if binding.len() != params.len() {
									return Err(ErrorResponse::error(
										SqlState::SyntaxError,
										format!(
											"wrong number of parameters for prepared statement {}",
											bind.prepared_statement_name
										),
									)
									.into());
								}

								let binding_format_codes = match bind.param_format {
									BindFormat::All(format) => {
										vec![format; params.len()]
									}
									BindFormat::PerColumn(format) => format,
								};

								tracing::debug!("Binding Parameters {} {:?}", self.id, &binding);
								tracing::debug!("Binding Param FormatCode {} {:?}", self.id, &binding_format_codes);
								tracing::debug!("Binding Result FormatCode {} {:?}", self.id, &result_format);

								if binding_format_codes.len() != binding.len() {
									return Err(ErrorResponse::error(
										SqlState::SyntaxError,
										format!(
											"wrong number of parameter format codes for prepared statement {}",
											bind.prepared_statement_name
										),
									)
									.into());
								}

								let portal = self
									.engine
									.create_portal(&bind.prepared_statement_name, params, binding, binding_format_codes)
									.await?;

								let row_desc = RowDescription {
									fields: prepared.fields.to_owned(),
									format_code: result_format,
								};

								let portal = BoundPortal { portal, row_desc };

								self.portals.insert(bind.portal, portal);
							}
							Err(err) => {
								tracing::error!("Connection.Bind {} {:?}", self.id, err);
								tracing::warn!(
									"Connection.Bind {} Missing Statement for {}",
									self.id,
									&bind.prepared_statement_name
								);
								return Err(err);
							}
						}

						tracing::debug!("Connection.BindComplete {}", self.id);
						framed.send(BindComplete).await?;
					}
					ClientMessage::Describe(Describe::PreparedStatement(ref statement_name)) => {
						tracing::debug!("Connection.DescribePreparedStatement {}", self.id);

						let prepared_statement = self.prepared_statement(statement_name)?;

						let parameters = prepared_statement.parameters.clone();
						let fields = prepared_statement.fields.clone();

						framed.send(ParameterDescription { parameters }).await?;

						framed
							.send(RowDescription {
								fields,
								format_code: FormatCode::Text,
							})
							.await?;
					}
					ClientMessage::Describe(Describe::Portal(ref name)) => match self.portal(name) {
						Ok(portal) => {
							tracing::debug!("Connection.DescribePortal {}", self.id);
							tracing::debug!("Portal Name {:?}", name);
							framed.send(portal.row_desc.clone()).await?;
						}
						Err(err) => {
							tracing::error!("Connection.DescribePortal {} {:?}", self.id, err);
							tracing::warn!("Connection.Bind {} Missing Portal for {}", self.id, name);
							framed.send(NoData).await?;
						}
					},
					ClientMessage::Sync => {
						tracing::debug!("Connection.Sync {}", self.id);
						framed.send(ReadyForQuery).await?;
					}
					ClientMessage::Execute(exec) => {
						tracing::debug!("Connection.Execute {}", &self.id);

						match self.portal_mut(&exec.portal) {
							Ok(bound) => {
								let mut batch_writer = DataRowBatch::from_row_desc(&bound.row_desc);

								bound.portal.execute(&mut batch_writer).await?;

								let num_rows = batch_writer.num_rows();

								framed.send(batch_writer).await?;

								framed
									.send(CommandComplete {
										command_tag: format!("SELECT {}", num_rows),
									})
									.await?;
							}
							Err(err) => {
								tracing::error!("Connection.Execute {} {:?}", self.id, err);
								tracing::warn!("Connection.Bind {} Missing Statement for {}", self.id, &exec.portal);
								framed.send(EmptyQueryResponse).await?;
							}
						}
					}
					ClientMessage::Query(query) => {
						tracing::debug!("Connection.Query {}", self.id);

						if let Some(parsed) = self.parse_statement(&query)? {
							let format_code = FormatCode::Text;
							let mut batch_writer = DataRowBatch::new(format_code);

							let fields = self.engine.query(&parsed, &mut batch_writer).await?;
							let num_rows = batch_writer.num_rows();

							let row_desc = RowDescription { fields, format_code };

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
					ClientMessage::Close(Close::Portal(ref _portal_name)) => {
						tracing::debug!("Connection.ClosePortal");
						// TODO
					}
					ClientMessage::Close(Close::PreparedStatement(ref _statement_name)) => {
						tracing::debug!("Connection.ClosePreparedStatement");
						// TODO
					}
					_ => return Err(ErrorResponse::error(SqlState::ProtocolViolation, "unexpected message").into()),
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
				Err(ConnectionError::ErrorResponse(err)) => {
					tracing::warn!("ConnectionError::ErrorResponse {} {:?}", self.id, err);

					framed.send(err.clone()).await?;

					if err.severity == Severity::Fatal {
						tracing::error!("Error not recoverable");
						return Err(err.into());
					}

					tracing::info!("Connection::ReadyForQuery {}", self.id);

					framed.send(ReadyForQuery).await?;
					ConnectionState::Idle
				}
				Err(err) => {
					tracing::error!("ConnectionError {} {:?}", self.id, err);

					framed
						.send(ErrorResponse::fatal(SqlState::ConnectionException, "connection error"))
						.await?;
					return Err(err);
				}
			};

			self.state = new_state;
		}
	}
}
