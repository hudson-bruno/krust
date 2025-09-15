use std::{io, mem};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct KafkaRequestBody {
    pub client_id: String,
    pub client_software_version: String,
    pub tag_buffer: i8,
}

impl KafkaRequestBody {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let client_id_size: usize = reader.read_i8().await?.try_into().unwrap();
        let mut client_id_bytes = vec![0u8; client_id_size - 1];
        reader.read_exact(&mut client_id_bytes).await?;
        let client_id: String = String::from_utf8(client_id_bytes).unwrap();

        let client_software_version_size: usize = reader.read_i8().await?.try_into().unwrap();
        let mut client_software_version_bytes = vec![0u8; client_software_version_size - 1];
        reader
            .read_exact(&mut client_software_version_bytes)
            .await?;
        let client_software_version: String =
            String::from_utf8(client_software_version_bytes).unwrap();

        let tag_buffer: i8 = reader.read_i8().await?;

        Ok(Self {
            client_id,
            client_software_version,
            tag_buffer,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        let client_id_size: usize = self.client_id.len() + 1;
        writer.write_i8(client_id_size.try_into().unwrap()).await?;
        writer.write_all(self.client_id.as_bytes()).await?;

        let client_software_version_size: usize = self.client_software_version.len() + 1;
        writer
            .write_i8(client_software_version_size.try_into().unwrap())
            .await?;
        writer
            .write_all(self.client_software_version.as_bytes())
            .await?;

        writer.write_i8(self.tag_buffer).await?;

        Ok(())
    }
}

impl Serializable for KafkaRequestBody {
    fn size(&self) -> usize {
        let client_id_size = mem::size_of::<i8>() + self.client_id.len();
        let client_software_version_size =
            mem::size_of::<i8>() + self.client_software_version.len();

        client_id_size + client_software_version_size + mem::size_of::<i8>()
    }
}
