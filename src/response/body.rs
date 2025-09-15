use std::{io, mem};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Serializable;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct KafkaResponseBody {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time: i32,
    pub tag_buffer: i8,
}

impl KafkaResponseBody {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let error_code: i16 = reader.read_i16().await?;

        let mut api_versions: Vec<ApiVersion> = vec![];
        let api_versions_size: usize = reader.read_i8().await?.try_into().unwrap();
        for _ in 1..api_versions_size {
            let api_version = ApiVersion::from_reader(reader).await?;
            api_versions.push(api_version);
        }

        let throttle_time: i32 = reader.read_i32().await?;
        let tag_buffer: i8 = reader.read_i8().await?;

        Ok(Self {
            error_code,
            api_versions,
            throttle_time,
            tag_buffer,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i16(self.error_code).await?;

        let api_versions_size: usize = self.api_versions.len() + 1;
        writer
            .write_i8(api_versions_size.try_into().unwrap())
            .await?;

        for api_version in &self.api_versions {
            api_version.write_into(writer).await?
        }

        writer.write_i32(self.throttle_time).await?;
        writer.write_i8(self.tag_buffer).await?;

        Ok(())
    }
}

impl Serializable for KafkaResponseBody {
    fn size(&self) -> usize {
        mem::size_of::<i16>()
            + self.api_versions.size()
            + mem::size_of::<i32>()
            + mem::size_of::<i8>()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_supported_api_version: i16,
    pub max_supported_api_version: i16,
    pub tag_buffer: i8,
}

impl ApiVersion {
    pub async fn from_reader<R>(reader: &mut R) -> Result<Self, io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let api_key: i16 = reader.read_i16().await?;
        let min_supported_api_version: i16 = reader.read_i16().await?;
        let max_supported_api_version: i16 = reader.read_i16().await?;
        let tag_buffer: i8 = reader.read_i8().await?;

        Ok(Self {
            api_key,
            min_supported_api_version,
            max_supported_api_version,
            tag_buffer,
        })
    }

    pub async fn write_into<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        writer.write_i16(self.api_key).await?;
        writer.write_i16(self.min_supported_api_version).await?;
        writer.write_i16(self.max_supported_api_version).await?;
        writer.write_i8(self.tag_buffer).await?;

        Ok(())
    }
}

impl Serializable for Vec<ApiVersion> {
    fn size(&self) -> usize {
        let api_version_size = mem::size_of::<i16>()
            + mem::size_of::<i16>()
            + mem::size_of::<i16>()
            + mem::size_of::<i8>();

        mem::size_of::<i8>() + (self.len() * api_version_size)
    }
}
