use std::io;

use bytes::BytesMut;
use codecrafters_kafka::{request::ApiVersionsRequest, response::ApiVersionsResponse, serde_kafka};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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

    pub async fn parse_response(&mut self) -> io::Result<ApiVersionsResponse> {
        let message_size: i32 = self.client_io.read_i32().await.unwrap();
        let mut message_bytes = vec![0u8; message_size.try_into().unwrap()];
        self.client_io.read_exact(&mut message_bytes).await.unwrap();

        Ok(serde_kafka::from_bytes(&message_bytes).unwrap())
    }

    pub async fn send_request(&mut self, request: &ApiVersionsRequest) -> io::Result<()> {
        let response_bytes = serde_kafka::to_bytes_mut(request).unwrap();
        let mut result = BytesMut::new();
        result.extend_from_slice(&(response_bytes.len() as i32).to_be_bytes());
        result.extend_from_slice(&response_bytes);
        self.client_io.write_all_buf(&mut result).await.unwrap();

        Ok(())
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.serve_handle.abort();
    }
}
