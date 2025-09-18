use std::{fmt::Debug, io, net::SocketAddr};

use serde::Serialize;
use tokio::{
    io::AsyncWriteExt,
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
    modules::{api_versions, describe_topic_partitions},
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

            handle_package(&mut io, start_time).await;
        }
    });
}

async fn handle_package(io: &mut TcpStream, start_time: Instant) {
    let (header, raw_body): (RequestHeaderV2, Vec<u8>) =
        serde_kafka::from_async_reader_trail_with_message_size(io)
            .await
            .unwrap();

    tracing::debug!("header: {:?}", header);

    match header.api_key {
        ApiKey::ApiVersions => {
            send_response(io, api_versions::handler(&header, raw_body), start_time).await
        }
        ApiKey::DescribeTopicPartitions => {
            send_response(
                io,
                describe_topic_partitions::handler(&header, raw_body),
                start_time,
            )
            .await
        }
        _ => panic!("Api key not found"),
    };
}

async fn send_response<I, S>(io: &mut I, response: S, start_time: Instant)
where
    I: AsyncWriteExt + Unpin,
    S: Serialize + Debug,
{
    serde_kafka::to_async_writer_with_message_size(io, &response)
        .await
        .unwrap();

    let elapsed_time = start_time.elapsed();
    tracing::debug!("response: {:?} elapsed: {:?}", response, elapsed_time);
}
