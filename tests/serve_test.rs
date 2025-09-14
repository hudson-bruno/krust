use codecrafters_kafka::request::{KafkaRequest, KafkaRequestHeader};

mod common;
use common::TestContext;

#[tokio::test]
async fn test_response_same_request_correlation_id() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest {
        message_size: 0,
        header: KafkaRequestHeader {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 7,
        },
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(
        request.header.correlation_id,
        response.header.correlation_id
    );
}

#[tokio::test]
async fn test_response_error_code() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest {
        message_size: 0,
        header: KafkaRequestHeader {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 7,
        },
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, 35);
}
