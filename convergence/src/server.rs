//! Contains utility types and functions for starting and running servers.

use crate::connection::{Connection, ConnectionError};
use crate::engine::Engine;
use crate::protocol::ProtocolError;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::{TcpListener, UnixListener};

/// Controls how servers bind to local network resources.
#[derive(Default)]
pub struct BindOptions {
	addr: String,
	port: u16,
	path: Option<String>,
}

impl BindOptions {
	/// Creates a default set of options listening only on the loopback address
	/// using the default Postgres port of 5432.
	pub fn new() -> Self {
		Self {
			addr: "127.0.0.1".to_owned(),
			port: 5432,
			path: None,
		}
	}

	/// Sets the port to be used.
	pub fn with_port(mut self, port: u16) -> Self {
		self.port = port;
		self
	}

	/// Sets the address to be used.
	pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
		self.addr = addr.into();
		self
	}

	pub fn with_socket_path(mut self, path: impl Into<String>) -> Self {
		self.path = Some(path.into());
		self
	}

	pub fn use_socket(&self) -> bool {
		self.path.is_some()
	}

	pub fn use_tcp(&self) -> bool {
		self.path.is_none()
	}

	/// Configures the server to listen on all interfaces rather than any specific address.
	pub fn use_all_interfaces(self) -> Self {
		self.with_addr("0.0.0.0")
	}
}

type EngineFunc<E> = Arc<dyn Fn() -> Pin<Box<dyn futures::Future<Output = E> + Send>> + Send + Sync>;

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	if bind.use_socket() {
		run_with_socket(bind, engine_func).await
	} else {
		run_with_tcp(bind, engine_func).await
	}
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_tcp<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	tracing::info!("Starting CipherStash on port {}", bind.port);

	let listener = TcpListener::bind((bind.addr, bind.port)).await?;

	loop {
		let (stream, _) = listener.accept().await?;

		let engine_func = engine_func.clone();
		tokio::spawn(async move {
			let mut conn = Connection::new(engine_func().await);
			let result = conn.run(stream).await;

			if let Err(ConnectionError::ConnectionClosed) = result {
				//Broken Pipe - client disconnected
				tracing::warn!("Connection closed by client");
			}
		});
	}
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run_with_socket<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	let path = bind.path.unwrap();

	fs::remove_file(&path).unwrap_or_default(); // Remove the file if it already exists

	tracing::info!("Starting CipherStash on socket {}", path);

	let listener = UnixListener::bind(path).unwrap();

	loop {
		let (stream, _) = listener.accept().await?;

		let engine_func = engine_func.clone();
		tokio::spawn(async move {
			let mut conn = Connection::new(engine_func().await);

			let result = conn.run(stream).await;

			if let Err(ConnectionError::Protocol(ProtocolError::Io(e))) = result {
				//Broken Pipe - client disconnected
				tracing::error!("Connection error: {}", e);
			}
		});
	}
}
