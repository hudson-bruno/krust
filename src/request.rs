use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaRequest {
    pub message_size: i32,
    pub header: KafkaRequestHeader,
}

impl KafkaRequest {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let message_size: i32 = reader.read_i32().await?;
        let header = KafkaRequestHeader::from_reader(reader).await?;

        Ok(Self {
            message_size,
            header,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i32(self.message_size).await?;
        self.header.write_into(writer).await?;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaRequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl KafkaRequestHeader {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let request_api_key: i16 = reader.read_i16().await?;
        let request_api_version: i16 = reader.read_i16().await?;
        let correlation_id: i32 = reader.read_i32().await?;

        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i16(self.request_api_key).await?;
        writer.write_i16(self.request_api_version).await?;
        writer.write_i32(self.correlation_id).await?;

        Ok(())
    }
}
