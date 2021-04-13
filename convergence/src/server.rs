//! Contains utility types and functions for starting and running servers.

use crate::connection::Connection;
use crate::engine::Engine;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Controls how servers bind to local network resources.
#[derive(Default)]
pub struct BindOptions {
	addr: String,
	port: u16,
}

impl BindOptions {
	/// Creates a default set of options listening only on the loopback address
	/// using the default Postgres port of 5432.
	pub fn new() -> Self {
		Self {
			addr: "127.0.0.1".to_owned(),
			port: 5432,
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

	/// Configures the server to listen on all interfaces rather than any specific address.
	pub fn use_all_interfaces(self) -> Self {
		self.with_addr("0.0.0.0")
	}
}

type EngineFunc<E> = Arc<dyn Fn() -> Pin<Box<dyn futures::Future<Output = E> + Send>> + Send + Sync>;

async fn run_with_listener<E: Engine>(listener: TcpListener, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	loop {
		let (stream, _) = listener.accept().await?;
		let engine_func = engine_func.clone();
		tokio::spawn(async move {
			let mut conn = Connection::new(engine_func().await);
			conn.run(stream).await.unwrap();
		});
	}
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Does not return unless the server terminates entirely.
pub async fn run<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<()> {
	let listener = TcpListener::bind((bind.addr, bind.port)).await?;
	run_with_listener(listener, engine_func).await
}

/// Starts a server using a function responsible for producing engine instances and set of bind options.
///
/// Returns once the server is listening for connections, with the accept loop
/// running as a background task, and returns the listener's local port.
///
/// Useful for creating test harnesses binding to port 0 to select a random port.
pub async fn run_background<E: Engine>(bind: BindOptions, engine_func: EngineFunc<E>) -> std::io::Result<u16> {
	let listener = TcpListener::bind((bind.addr, bind.port)).await?;
	let port = listener.local_addr()?.port();

	tokio::spawn(async move { run_with_listener(listener, engine_func).await });

	Ok(port)
}
