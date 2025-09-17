use codecrafters_kafka::{
    constants::{ApiKey, ErrorCode},
    request::{ApiVersionsRequest, RequestHeaderV2},
};

mod common;
use common::TestContext;

#[tokio::test]
async fn test_response_same_request_correlation_id() {
    let mut ctx = TestContext::new().await;

    let request = ApiVersionsRequest::default();
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

    let request = ApiVersionsRequest {
        header: RequestHeaderV2 {
            api_key: ApiKey::Invalid,
            ..RequestHeaderV2::default()
        },
        ..ApiVersionsRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, ErrorCode::UnsupportedVersion);
}

#[tokio::test]
async fn test_unsupported_api_versions_version() {
    let mut ctx = TestContext::new().await;

    let request = ApiVersionsRequest {
        header: RequestHeaderV2 {
            api_key: ApiKey::ApiVersions,
            api_version: 5,
            ..RequestHeaderV2::default()
        },
        ..ApiVersionsRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, ErrorCode::UnsupportedVersion);
}

#[tokio::test]
async fn test_api_versions() {
    let mut ctx = TestContext::new().await;

    let request = ApiVersionsRequest {
        header: RequestHeaderV2 {
            api_key: ApiKey::ApiVersions,
            ..RequestHeaderV2::default()
        },
        ..ApiVersionsRequest::default()
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_response().await.unwrap();

    assert_eq!(response.body.error_code, ErrorCode::NoError);
    assert_eq!(
        response.header.correlation_id,
        request.header.correlation_id
    );

    assert_eq!(response.body.api_versions[0].api_key, ApiKey::Fetch);
    assert_eq!(response.body.api_versions[0].max_supported_api_version, 17);

    assert_eq!(response.body.api_versions[1].api_key, ApiKey::ApiVersions);
    assert_eq!(response.body.api_versions[1].max_supported_api_version, 4);

    assert_eq!(
        response.body.api_versions[2].api_key,
        ApiKey::DescribeTopicPartitions
    );
}

#[tokio::test]
async fn test_serial_requests() {
    let mut ctx = TestContext::new().await;

    for _ in 0..2 {
        let request = ApiVersionsRequest::default();
        ctx.send_request(&request).await.unwrap();

        let response = ctx.parse_response().await.unwrap();

        assert_eq!(
            request.header.correlation_id,
            response.header.correlation_id
        );
    }
}
