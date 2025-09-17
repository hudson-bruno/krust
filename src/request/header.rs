use serde::{Deserialize, Serialize};

use crate::constants::ApiKey;

#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHeaderV2 {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tag_buffer: i8,
}
