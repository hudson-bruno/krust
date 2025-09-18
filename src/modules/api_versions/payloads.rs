use serde::{Deserialize, Serialize};

use crate::{
    constants::{ApiKey, ErrorCode},
    headers::ResponseHeaderV0,
    serde_kafka::CompactString,
};

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsRequestBody {
    pub client_id: CompactString,
    pub client_software_version: CompactString,
    pub tag_buffer: i8,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsResponse {
    pub header: ResponseHeaderV0,
    pub body: ApiVersionsResponseBody,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsResponseBody {
    pub error_code: ErrorCode,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time: i32,
    pub tag_buffer: i8,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersion {
    pub api_key: ApiKey,
    pub min_supported_api_version: i16,
    pub max_supported_api_version: i16,
    pub tag_buffer: i8,
}
