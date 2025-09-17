use serde::{Deserialize, Serialize};

mod body;
mod header;

pub use crate::response::{
    body::{ApiVersion, ApiVersionsResponseBody},
    header::ApiVersionsResponseHeader,
};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsResponse {
    pub header: ApiVersionsResponseHeader,
    pub body: ApiVersionsResponseBody,
}
