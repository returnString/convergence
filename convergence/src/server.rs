use crate::connection::{Connection, ConnectionError};
use crate::engine::Engine;
use tokio::net::TcpListener;

pub struct BindOptions {
	addr: String,
	port: u16,
}

impl BindOptions {
	pub fn new() -> Self {
		Self {
			addr: "127.0.0.1".to_owned(),
			port: 5432,
		}
	}

	pub fn with_port(mut self, port: u16) -> Self {
		self.port = port;
		self
	}

	pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
		self.addr = addr.into();
		self
	}

	pub fn use_all_interfaces(self) -> Self {
		self.with_addr("0.0.0.0")
	}
}

pub async fn run<E: Engine>(bind: BindOptions) -> Result<(), ConnectionError> {
	let listener = TcpListener::bind((bind.addr, bind.port)).await?;

	loop {
		let (stream, _) = listener.accept().await?;
		tokio::spawn(async move {
			let backend = E::new().await;
			let mut conn = Connection::new(stream, backend);
			conn.run().await.unwrap();
		});
	}
}

pub fn run_background<E: Engine>(bind: BindOptions) -> tokio::task::JoinHandle<Result<(), ConnectionError>> {
	tokio::spawn(async move { run::<E>(bind).await })
}
