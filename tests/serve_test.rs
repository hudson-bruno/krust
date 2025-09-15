use codecrafters_kafka::request::{KafkaRequest, KafkaRequestHeader};

mod common;
use common::TestContext;

#[tokio::test]
async fn test_response_same_request_correlation_id() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest::default();
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(
        request.header.correlation_id,
        response.header.correlation_id
    );
}

#[tokio::test]
async fn test_unsupported_api_key() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest {
        header: KafkaRequestHeader {
            api_key: 2,
            ..KafkaRequestHeader::default()
        },
        ..KafkaRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, 35);
}

#[tokio::test]
async fn test_unsupported_api_versions_version() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest {
        header: KafkaRequestHeader {
            api_key: 18,
            api_version: 5,
            ..KafkaRequestHeader::default()
        },
        ..KafkaRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, 35);
}

#[tokio::test]
async fn test_api_versions() {
    let mut ctx = TestContext::new().await;

    let request = KafkaRequest {
        header: KafkaRequestHeader {
            api_key: 18,
            ..KafkaRequestHeader::default()
        },
        ..KafkaRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, 0);
    assert_eq!(
        response.header.correlation_id,
        request.header.correlation_id
    );

    assert_eq!(response.body.api_versions[0].api_key, 1);
    assert_eq!(response.body.api_versions[0].max_supported_api_version, 17);

    assert_eq!(response.body.api_versions[1].api_key, 18);
    assert_eq!(response.body.api_versions[1].max_supported_api_version, 4);

    assert_eq!(response.body.api_versions[2].api_key, 75);
}

#[tokio::test]
async fn test_serial_requests() {
    let mut ctx = TestContext::new().await;

    for _ in 0..2 {
        let request = KafkaRequest::default();
        ctx.send_request(&request).await.unwrap();

        let response = ctx.parse_response().await.unwrap();

        assert_eq!(
            request.header.correlation_id,
            response.header.correlation_id
        );
    }
}
