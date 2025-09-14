use std::{io, net::SocketAddr};

use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod request;

use request::KafkaRequest;

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

        let (mut reader, mut writer) = io.split();

        let request = KafkaRequest::from_reader(&mut reader).await.unwrap();
        tracing::debug!("request: {:?}", request);

        let response = request;
        response.write_into(&mut writer).await.unwrap();

        let elapsed_time = start_time.elapsed();
        tracing::debug!("response: {:?} elapsed: {:?}", response, elapsed_time);
    });
}
