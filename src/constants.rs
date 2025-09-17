use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Eq, Debug, Default)]
#[repr(i16)]
pub enum ErrorCode {
    #[default]
    NoError = 0,
    UnsupportedVersion = 35,
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Eq, Debug, Default)]
#[repr(i16)]
pub enum ApiKey {
    #[default]
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}
