use std::{io, net::SocketAddr};

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod constants;
pub mod headers;
pub mod modules;
pub mod serde_kafka;

#[cfg(feature = "test-helpers")]
pub mod test_helpers;

use crate::{
    constants::ApiKey,
    headers::RequestHeaderV2,
    modules::api_versions::{self, payloads::ApiVersionsResponse},
};

pub fn serve(listener: TcpListener) -> Serve {
    Serve { listener }
}

pub struct Serve {
    listener: TcpListener,
}

impl Serve {
    pub async fn run(self) -> io::Result<()> {
        let Self { listener } = self;

        loop {
            let (io, remote_addr) = listener.accept().await?;

            handle_connection(io, remote_addr).await;
        }
    }
}

async fn handle_connection(mut io: TcpStream, remote_addr: SocketAddr) {
    tokio::spawn(async move {
        tracing::trace!("connection {remote_addr:?} accepted");

        loop {
            let start_time = Instant::now();

            let response = handle_package(&mut io).await;

            let elapsed_time = start_time.elapsed();
            tracing::debug!("response: {:?} elapsed: {:?}", response, elapsed_time);
        }
    });
}

async fn handle_package(io: &mut TcpStream) -> ApiVersionsResponse {
    let (header, raw_body): (RequestHeaderV2, Vec<u8>) =
        serde_kafka::from_async_reader_trail_with_message_size(io)
            .await
            .unwrap();

    tracing::debug!("header: {:?}", header);

    let response: ApiVersionsResponse = match header.api_key {
        ApiKey::ApiVersions => api_versions::handler(&header, raw_body),
        _ => panic!("Api key not found"),
    };

    serde_kafka::to_async_writer_with_message_size(io, &response)
        .await
        .unwrap();

    response
}
