use serde::{Deserialize, Serialize};

mod body;
mod header;

pub use crate::request::{body::ApiVersionsRequestBody, header::RequestHeaderV2};

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsRequest {
    pub header: RequestHeaderV2,
    pub body: ApiVersionsRequestBody,
}
