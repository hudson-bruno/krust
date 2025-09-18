use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    constants::ErrorCode, headers::ResponseHeaderV1, serde_kafka::uuid_as_bytes,
    serde_kafka::CompactString,
};

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeTopicPartitionsRequestBody {
    pub topics: Vec<TopicRequest>,
    pub response_partition_limit: i32,
    pub cursor: u8,
    pub tag_buffer: u8,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRequest {
    pub name: CompactString,
    pub tag_buffer: u8,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeTopicPartitionsResponse {
    pub header: ResponseHeaderV1,
    pub body: DescribeTopicPartitionsResponseBody,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeTopicPartitionsResponseBody {
    pub throttle_time: i32,
    pub topics: Vec<TopicResponse>,
    pub next_cursor: u8,
    pub tag_buffer: u8,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicResponse {
    pub error_code: ErrorCode,
    pub name: CompactString,
    #[serde(with = "uuid_as_bytes")]
    pub uuid: Uuid,
    pub is_internal: bool,
    pub partitions_array: u8,
    pub authorized_operations: u32,
    pub tag_buffer: u8,
}
