use std::io;

use codecrafters_kafka::{request::KafkaRequest, response::KafkaResponse};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

pub struct TestContext {
    pub serve_handle: JoinHandle<()>,
    pub client_io: TcpStream,
}

impl TestContext {
    pub async fn new() -> Self {
        let listener = TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let serve_handle = tokio::spawn(async {
            codecrafters_kafka::serve(listener).run().await.unwrap();
        });

        let client_io = TcpStream::connect(listener_addr).await.unwrap();

        Self {
            serve_handle,
            client_io,
        }
    }

    pub async fn parse_response(&mut self) -> io::Result<KafkaResponse> {
        KafkaResponse::from_reader(&mut self.client_io).await
    }

    pub async fn send_request(&mut self, request: &KafkaRequest) -> io::Result<()> {
        request.write_into(&mut self.client_io).await
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.serve_handle.abort();
    }
}
