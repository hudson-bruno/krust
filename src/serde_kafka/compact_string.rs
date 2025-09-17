use std::ops::Deref;

use serde::{Deserialize, Serialize};

pub const COMPACT_STRING_NAME: &str = "CompactString";

#[derive(Default, Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct CompactString(pub String);

impl Deref for CompactString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for CompactString {
    fn from(s: &str) -> Self {
        CompactString(s.to_string())
    }
}

impl From<String> for CompactString {
    fn from(s: String) -> Self {
        CompactString(s)
    }
}
