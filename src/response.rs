use std::io::{self, Cursor};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

mod body;
mod header;

pub use crate::response::{
    body::{ApiVersion, KafkaResponseBody},
    header::KafkaResponseHeader,
};

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaResponse {
    pub header: KafkaResponseHeader,
    pub body: KafkaResponseBody,
}

impl KafkaResponse {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let message_size: usize = reader.read_i32().await?.try_into().unwrap();

        let mut message_bytes = vec![0u8; message_size];
        reader.read_exact(&mut message_bytes).await?;

        let mut cursor = Cursor::new(message_bytes);

        let header = KafkaResponseHeader::from_reader(&mut cursor).await?;
        let body = KafkaResponseBody::from_reader(&mut cursor).await?;

        Ok(Self { header, body })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        let message_size: i32 = self.size().try_into().unwrap();
        writer.write_i32(message_size).await?;

        self.header.write_into(writer).await?;
        self.body.write_into(writer).await?;

        Ok(())
    }
}

impl Serializable for KafkaResponse {
    fn size(&self) -> usize {
        self.header.size() + self.body.size()
    }
}
