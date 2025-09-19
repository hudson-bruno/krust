use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::serde_kafka::CompactString;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataLogFile {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic_byte: u8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub length: i8,
    pub attributes: i8,
    pub timestamp_delta: i8,
    pub offset_delta: i8,
    pub keys: Vec<i8>,
    pub value: RecordValue,
    pub headers_array_count: u8,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordValue {
    FeatureLevelValue {
        frame_version: i8,
        value_type: i8,
        version: i8,
        name: CompactString,
        feature_level: i16,
        tagged_fields_count: u8,
    },
    TopicRecordValue {
        frame_version: i8,
        value_type: i8,
        version: i8,
        topic_name: CompactString,
        topic_uuid: Uuid,
        tagged_fields_count: u8,
    },
    PartitionRecordValue {
        frame_version: i8,
        value_type: i8,
        version: i8,
        partition_id: i32,
        topic_uuid: Uuid,
        replicas: Vec<i32>,
        in_sync_replicas: Vec<i32>,
        removing_replicas: Vec<i32>,
        adding_replicas: Vec<i32>,
        leader: i32,
        leader_epoch: i32,
        partition_epoch: i32,
        directories: Vec<Uuid>,
        tagged_fields_count: u8,
    },
}
