use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaRequestHeader {
    pub correlation_id: i32,
}

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

        Ok(Self {
            message_size,
            header: KafkaRequestHeader { correlation_id: 7 },
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i32(self.message_size).await?;
        writer.write_i32(self.header.correlation_id).await?;

        Ok(())
    }
}
