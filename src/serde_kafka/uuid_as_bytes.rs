use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserializer, Serializer};
use std::fmt;
use uuid::Uuid;

pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut tuple = serializer.serialize_tuple(16)?;
    for byte in uuid.as_bytes().iter() {
        tuple.serialize_element(byte)?;
    }
    tuple.end()
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    struct UuidVisitor;

    impl<'de> Visitor<'de> for UuidVisitor {
        type Value = Uuid;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a 16-byte array for Uuid")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = [0u8; 16];
            for (i, byte) in bytes.iter_mut().enumerate() {
                *byte = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
            }
            Ok(Uuid::from_bytes(bytes))
        }
    }

    deserializer.deserialize_tuple(16, UuidVisitor)
}
