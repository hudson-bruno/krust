use std::{io, net::SocketAddr};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod request;
pub mod response;

use request::KafkaRequest;
use response::KafkaResponse;

use crate::response::{body::KafkaResponseBody, header::KafkaResponseHeader};

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

        let start_time = Instant::now();

        let request = KafkaRequest::from_reader(&mut io).await.unwrap();
        tracing::debug!("request: {:?}", request);

        let response: KafkaResponse = KafkaResponse {
            header: KafkaResponseHeader {
                correlation_id: request.header.correlation_id,
            },
            body: KafkaResponseBody { error_code: 35 },
        };
        response.write_into(&mut io).await.unwrap();

        io.shutdown().await.unwrap();

        let elapsed_time = start_time.elapsed();
        tracing::debug!("response: {:?} elapsed: {:?}", response, elapsed_time);
    });
}

pub trait Serializable {
    fn size(&self) -> usize;
}
