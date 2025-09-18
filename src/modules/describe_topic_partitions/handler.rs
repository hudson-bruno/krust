use uuid::Uuid;

use crate::{
    constants::ErrorCode,
    headers::{RequestHeaderV2, ResponseHeaderV1},
    modules::describe_topic_partitions::payloads::{
        DescribeTopicPartitionsRequestBody, DescribeTopicPartitionsResponse,
        DescribeTopicPartitionsResponseBody, TopicResponse,
    },
    serde_kafka,
};

pub fn handler(header: &RequestHeaderV2, raw_body: Vec<u8>) -> DescribeTopicPartitionsResponse {
    let body: DescribeTopicPartitionsRequestBody = serde_kafka::from_bytes(&raw_body).unwrap();

    DescribeTopicPartitionsResponse {
        header: ResponseHeaderV1 {
            correlation_id: header.correlation_id,
            ..Default::default()
        },
        body: DescribeTopicPartitionsResponseBody {
            topics: vec![TopicResponse {
                error_code: ErrorCode::UnknownTopic,
                name: body.topics[0].name.clone(),
                uuid: Uuid::nil(),
                partitions_array: 1,
                authorized_operations: 0x00_00_0d_f8,
                ..Default::default()
            }],
            next_cursor: 0xff,
            ..Default::default()
        },
    }
}
