use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiVersionsResponseHeader {
    pub correlation_id: i32,
}
