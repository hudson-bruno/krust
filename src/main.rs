use std::{io, net::SocketAddr, str::FromStr};

use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> io::Result<()> {
    init_tracing();

    let addr = SocketAddr::from_str("127.0.0.1:9092").unwrap();

    let listener = TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Server listening on {addr}");

    codecrafters_kafka::serve(listener).run().await?;

    Ok(())
}

pub fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}
