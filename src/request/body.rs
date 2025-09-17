use serde::{Deserialize, Serialize};

use crate::serde_kafka::CompactString;

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsRequestBody {
    pub client_id: CompactString,
    pub client_software_version: CompactString,
    pub tag_buffer: i8,
}
