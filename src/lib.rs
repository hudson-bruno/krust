use std::{io, net::SocketAddr};

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod request;
pub mod response;

use request::KafkaRequest;
use response::KafkaResponse;

use crate::response::{ApiVersion, KafkaResponseBody, KafkaResponseHeader};

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

async fn handle_package(mut io: &mut TcpStream) -> KafkaResponse {
    let request = KafkaRequest::from_reader(&mut io).await.unwrap();
    tracing::debug!("request: {:?}", request);

    let response = if request.header.api_key == 18
        && request.header.api_version >= 0
        && request.header.api_version <= 4
    {
        KafkaResponse {
            header: KafkaResponseHeader {
                correlation_id: request.header.correlation_id,
            },
            body: KafkaResponseBody {
                api_versions: vec![
                    ApiVersion {
                        api_key: 1,
                        max_supported_api_version: 17,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: 18,
                        max_supported_api_version: 4,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: 75,
                        ..ApiVersion::default()
                    },
                ],
                ..KafkaResponseBody::default()
            },
        }
    } else {
        KafkaResponse {
            header: KafkaResponseHeader {
                correlation_id: request.header.correlation_id,
            },
            body: KafkaResponseBody {
                error_code: 35,
                ..KafkaResponseBody::default()
            },
        }
    };

    response.write_into(&mut io).await.unwrap();

    response
}

pub trait Serializable {
    fn size(&self) -> usize;
}
