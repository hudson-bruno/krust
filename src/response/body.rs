use std::{io, mem};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaResponseBody {
    pub error_code: i16,
}

impl KafkaResponseBody {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let error_code: i16 = reader.read_i16().await?;

        Ok(Self { error_code })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i16(self.error_code).await?;

        Ok(())
    }
}

impl Serializable for KafkaResponseBody {
    fn size(&self) -> usize {
        mem::size_of::<i16>()
    }
}
