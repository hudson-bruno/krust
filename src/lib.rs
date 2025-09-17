use std::{io, net::SocketAddr};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod constants;
pub mod request;
pub mod response;
pub mod serde_kafka;

use request::ApiVersionsRequest;
use response::ApiVersionsResponse;

use crate::{
    constants::{ApiKey, ErrorCode},
    response::{ApiVersion, ApiVersionsResponseBody, ApiVersionsResponseHeader},
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
    let message_size: i32 = io.read_i32().await.unwrap();
    let mut message_bytes = vec![0u8; message_size.try_into().unwrap()];
    io.read_exact(&mut message_bytes).await.unwrap();
    let request: ApiVersionsRequest = serde_kafka::from_bytes(&message_bytes).unwrap();

    tracing::debug!("request: {:?}", request);

    let response = if request.header.api_key == ApiKey::ApiVersions
        && request.header.api_version >= 0
        && request.header.api_version <= 4
    {
        ApiVersionsResponse {
            header: ApiVersionsResponseHeader {
                correlation_id: request.header.correlation_id,
            },
            body: ApiVersionsResponseBody {
                api_versions: vec![
                    ApiVersion {
                        api_key: ApiKey::Fetch,
                        max_supported_api_version: 17,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: ApiKey::ApiVersions,
                        max_supported_api_version: 4,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: ApiKey::DescribeTopicPartitions,
                        ..ApiVersion::default()
                    },
                ],
                ..ApiVersionsResponseBody::default()
            },
        }
    } else {
        ApiVersionsResponse {
            header: ApiVersionsResponseHeader {
                correlation_id: request.header.correlation_id,
            },
            body: ApiVersionsResponseBody {
                error_code: ErrorCode::UnsupportedVersion,
                ..ApiVersionsResponseBody::default()
            },
        }
    };

    let response_bytes = serde_kafka::to_bytes_mut(&response).unwrap();
    let mut result = BytesMut::new();
    result.extend_from_slice(&(response_bytes.len() as i32).to_be_bytes());
    result.extend_from_slice(&response_bytes);
    io.write_all_buf(&mut result).await.unwrap();

    response
}
