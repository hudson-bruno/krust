use std::{io, mem};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaResponseHeader {
    pub correlation_id: i32,
}

impl KafkaResponseHeader {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let correlation_id: i32 = reader.read_i32().await?;

        Ok(Self { correlation_id })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i32(self.correlation_id).await?;

        Ok(())
    }
}

impl Serializable for KafkaResponseHeader {
    fn size(&self) -> usize {
        mem::size_of::<i32>()
    }
}
