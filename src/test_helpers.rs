use std::io;

use crate::serde_kafka;
use serde::{de::DeserializeOwned, Serialize};
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
            crate::serve(listener).run().await.unwrap();
        });

        let client_io = TcpStream::connect(listener_addr).await.unwrap();

        Self {
            serve_handle,
            client_io,
        }
    }

    pub async fn parse_response<D>(&mut self) -> io::Result<D>
    where
        D: DeserializeOwned,
    {
        Ok(
            serde_kafka::from_async_reader_with_message_size(&mut self.client_io)
                .await
                .unwrap(),
        )
    }

    pub async fn send_request<S>(&mut self, request: &S) -> serde_kafka::Result<()>
    where
        S: Serialize,
    {
        serde_kafka::to_async_writer_with_message_size(&mut self.client_io, &request).await
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.serve_handle.abort();
    }
}
