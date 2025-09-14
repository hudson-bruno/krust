use std::{io, mem};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct KafkaRequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tag_buffer: i8,
}

impl KafkaRequestHeader {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let api_key: i16 = reader.read_i16().await?;
        let api_version: i16 = reader.read_i16().await?;
        let correlation_id: i32 = reader.read_i32().await?;

        let client_id_size: usize = reader.read_i16().await?.try_into().unwrap();
        let mut string_bytes = vec![0u8; client_id_size];
        reader.read_exact(&mut string_bytes).await?;
        let client_id: String = String::from_utf8(string_bytes).unwrap();

        let tag_buffer: i8 = reader.read_i8().await?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i16(self.api_key).await?;
        writer.write_i16(self.api_version).await?;
        writer.write_i32(self.correlation_id).await?;

        writer
            .write_i16(self.client_id.len().try_into().unwrap())
            .await?;
        writer.write_all(self.client_id.as_bytes()).await?;

        writer.write_i8(self.tag_buffer).await?;

        Ok(())
    }
}

impl Serializable for KafkaRequestHeader {
    fn size(&self) -> usize {
        let client_id_size = mem::size_of::<i16>() + self.client_id.len();

        mem::size_of::<i16>()
            + mem::size_of::<i16>()
            + mem::size_of::<i32>()
            + client_id_size
            + mem::size_of::<i8>()
    }
}
