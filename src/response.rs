use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    response::{body::KafkaResponseBody, header::KafkaResponseHeader},
    Serializable,
};

pub mod body;
pub mod header;

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
        let _message_size: i32 = reader.read_i32().await?;
        let header = KafkaResponseHeader::from_reader(reader).await?;
        let body = KafkaResponseBody::from_reader(reader).await?;

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
