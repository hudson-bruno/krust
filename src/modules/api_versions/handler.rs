use crate::{
    constants::{ApiKey, ErrorCode},
    headers::{RequestHeaderV2, ResponseHeaderV0},
    modules::api_versions::payloads::{
        ApiVersion, ApiVersionsRequestBody, ApiVersionsResponse, ApiVersionsResponseBody,
    },
    serde_kafka,
};

pub fn handler(header: &RequestHeaderV2, raw_body: Vec<u8>) -> ApiVersionsResponse {
    let _body: ApiVersionsRequestBody = serde_kafka::from_bytes(&raw_body).unwrap();

    match header.api_version {
        0..=4 => ApiVersionsResponse {
            header: ResponseHeaderV0 {
                correlation_id: header.correlation_id,
            },
            body: ApiVersionsResponseBody {
                api_versions: vec![
                    ApiVersion {
                        api_key: ApiKey::Fetch,
                        max_supported_api_version: 17,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: ApiKey::ApiVersions,
                        max_supported_api_version: 4,
                        ..ApiVersion::default()
                    },
                    ApiVersion {
                        api_key: ApiKey::DescribeTopicPartitions,
                        ..ApiVersion::default()
                    },
                ],
                ..ApiVersionsResponseBody::default()
            },
        },
        _ => ApiVersionsResponse {
            header: ResponseHeaderV0 {
                correlation_id: header.correlation_id,
            },
            body: ApiVersionsResponseBody {
                error_code: ErrorCode::UnsupportedVersion,
                ..ApiVersionsResponseBody::default()
            },
        },
    }
}
