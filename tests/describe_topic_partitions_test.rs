use serde::{Deserialize, Serialize};

use codecrafters_kafka::{
    constants::{ApiKey, ErrorCode},
    headers::RequestHeaderV2,
    modules::describe_topic_partitions::payloads::{
        DescribeTopicPartitionsRequestBody, DescribeTopicPartitionsResponse, TopicRequest,
    },
    test_helpers::TestContext,
};
use uuid::Uuid;

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescribeTopicPartitionsRequest {
    pub header: RequestHeaderV2,
    pub body: DescribeTopicPartitionsRequestBody,
}

#[tokio::test]
async fn test_unknown_topic() {
    let mut ctx = TestContext::new().await;

    let request = DescribeTopicPartitionsRequest {
        header: RequestHeaderV2 {
            api_key: ApiKey::DescribeTopicPartitions,
            ..RequestHeaderV2::default()
        },
        body: DescribeTopicPartitionsRequestBody {
            topics: vec![TopicRequest {
                name: "foo".into(),
                ..Default::default()
            }],
            response_partition_limit: 100,
            cursor: 0xff,
            ..Default::default()
        },
    };
    ctx.send_request(&request).await.unwrap();

    let response: DescribeTopicPartitionsResponse = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.topics[0].uuid, Uuid::nil());

    assert_eq!(
        request.header.correlation_id,
        response.header.correlation_id
    );
    assert_eq!(response.body.topics[0].error_code, ErrorCode::UnknownTopic);
    assert_eq!(response.body.topics[0].name, response.body.topics[0].name);
}
