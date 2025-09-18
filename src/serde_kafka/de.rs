use std::{num::TryFromIntError, string::FromUtf8Error};

use bytes::Buf;
use serde::{
    de::{self, DeserializeOwned, IntoDeserializer, Visitor},
    Deserialize,
};
use tokio::io::AsyncReadExt;

use super::COMPACT_STRING_NAME;

use super::error::{Error, Result};

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }
}

pub fn from_bytes_trail<'a, T>(s: &'a [u8]) -> Result<(T, Vec<u8>)>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(s);
    let t = T::deserialize(&mut deserializer)?;

    Ok((t, deserializer.input.to_owned()))
}

pub fn from_bytes<'a, T>(s: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}

pub async fn from_async_reader_with_message_size<R, D>(reader: &mut R) -> Result<D>
where
    R: AsyncReadExt + Unpin,
    D: DeserializeOwned,
{
    let message_size: i32 = reader.read_i32().await.unwrap();
    let mut message_bytes = vec![0u8; message_size.try_into().unwrap()];
    reader.read_exact(&mut message_bytes).await.unwrap();

    from_bytes(&message_bytes)
}

pub async fn from_async_reader_trail_with_message_size<R, D>(reader: &mut R) -> Result<(D, Vec<u8>)>
where
    R: AsyncReadExt + Unpin,
    D: DeserializeOwned,
{
    let message_size: i32 = reader.read_i32().await.unwrap();
    let mut message_bytes = vec![0u8; message_size.try_into().unwrap()];
    reader.read_exact(&mut message_bytes).await.unwrap();

    from_bytes_trail(&message_bytes)
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_u8();
        visitor.visit_bool(value != 0)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_i8();
        visitor.visit_i8(value)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_i16();
        visitor.visit_i16(value)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_i32();
        visitor.visit_i32(value)
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_u8();
        visitor.visit_u8(value)
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.input.get_u32();
        visitor.visit_u32(value)
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let stored_length = self.input.get_i16();

        let length = stored_length as usize;

        if length == 0 {
            return visitor.visit_string("".into());
        }

        if self.input.remaining() < length {
            return Err(Error::Message("Unexpected EOF".to_string()));
        }

        let string_bytes = &self.input[..length];

        let string = String::from_utf8(string_bytes.to_vec())
            .map_err(|e: FromUtf8Error| Error::Message(e.to_string()))?;
        self.input.advance(length);

        visitor.visit_string(string)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let length = self
            .input
            .get_i32()
            .try_into()
            .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

        if self.input.remaining() < length {
            return Err(Error::Message("Unexpected EOF".to_string()));
        }

        let bytes = &self.input[..length];
        self.input.advance(length);

        visitor.visit_bytes(bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if name == COMPACT_STRING_NAME {
            let stored_length = self.input.get_i8();

            if stored_length <= 0 {
                return Err(Error::Message(format!(
                    "Invalid length for non-optional compact string: {}",
                    stored_length
                )));
            }

            let length = (stored_length as usize) - 1;

            if self.input.remaining() < length {
                return Err(Error::Message("Unexpected EOF".to_string()));
            }

            let string_bytes = &self.input[..length];

            let s = String::from_utf8(string_bytes.to_vec())
                .map_err(|e| Error::Message(e.to_string()))?;

            self.input.advance(length);

            let de = s.into_deserializer();
            visitor.visit_newtype_struct(de)
        } else {
            visitor.visit_newtype_struct(self)
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let length: usize = self
            .input
            .get_i8()
            .try_into()
            .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

        visitor.visit_seq(SeqAccess {
            deserializer: self,
            len: length - 1,
        })
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SeqAccess {
            deserializer: self,
            len,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SeqAccess {
            deserializer: self,
            len: fields.len(),
        })
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }
}

struct SeqAccess<'a, 'de: 'a> {
    deserializer: &'a mut Deserializer<'de>,
    len: usize,
}

impl<'de, 'a> de::SeqAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.len > 0 {
            self.len -= 1;
            let value = de::DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod test {
    use crate::serde_kafka::CompactString;

    use super::*;

    #[derive(Deserialize, PartialEq, Debug)]
    struct Test {
        i8: i8,
        i16: i16,
        i32: i32,
        string: String,
        compact_string: CompactString,
        ca_i8: Vec<i8>,
        ca_i16: Vec<i16>,
        ca_i32: Vec<i32>,
        ca_string: Vec<String>,
        ca_struct: Vec<SubStruct>,
    }

    #[derive(Deserialize, PartialEq, Debug, Clone)]
    struct SubStruct {
        i8: i8,
        i16: i16,
        i32: i32,
    }

    #[test]
    fn test_struct() {
        let bytes: &[u8] = &[
            0x7f, // i8: 127
            0x7f, 0xff, //i16: 32767
            0x7f, 0xff, 0xff, 0xff, // i32: 2147483647
            0x00, 0x09, // str: len 10
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // str: kafka-cli
            // --- Added for compact_string ---
            0x06, // compact_string len: 6 (5+1)
            0x68, 0x65, 0x6c, 0x6c, 0x6f, // compact_string: "hello"
            // ---
            // compact array: [i8::MAX, i8::MAX, i8::MAX]
            0x04, // len 3
            0x7f, // i8::MAX
            0x7f, // i8::MAX
            0x7f, // i8::MAX
            // compact array: [i16::MAX, i16::MAX, i16::MAX]
            0x04, // len 3
            0x7f, 0xff, // i16::MAX
            0x7f, 0xff, // i16::MAX
            0x7f, 0xff, // i16::MAX
            // compact array: [i32::MAX, i32::MAX, i32::MAX]
            0x04, // len 3
            0x7f, 0xff, 0xff, 0xff, // i32::MAX
            0x7f, 0xff, 0xff, 0xff, // i32::MAX
            0x7f, 0xff, 0xff, 0xff, // i32::MAX
            // compact array: ["kafka-cli", "kafka-cli", "kafka-cli"]
            0x04, // len 3
            0x00, 0x09, // str: len 10
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // str: kafka-cli
            0x00, 0x09, // str: len 10
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // str: kafka-cli
            0x00, 0x09, // str: len 10
            0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // str: kafka-cli
            // compact array: [SubStruct, SubStruct, SubStruct]
            0x04, // len 3
            0x7f, // i8: 127
            0x7f, 0xff, //i16: 32767
            0x7f, 0xff, 0xff, 0xff, // i32: 2147483647
            0x7f, // i8: 127
            0x7f, 0xff, //i16: 32767
            0x7f, 0xff, 0xff, 0xff, // i32: 2147483647
            0x7f, // i8: 127
            0x7f, 0xff, //i16: 32767
            0x7f, 0xff, 0xff, 0xff, // i32: 2147483647
        ];

        let sub_struct_expected = SubStruct {
            i8: i8::MAX,
            i16: i16::MAX,
            i32: i32::MAX,
        };
        let expected = Test {
            i8: i8::MAX,
            i16: i16::MAX,
            i32: i32::MAX,
            string: "kafka-cli".into(),
            compact_string: CompactString("hello".into()),
            ca_i8: vec![i8::MAX, i8::MAX, i8::MAX],
            ca_i16: vec![i16::MAX, i16::MAX, i16::MAX],
            ca_i32: vec![i32::MAX, i32::MAX, i32::MAX],
            ca_string: vec!["kafka-cli".into(), "kafka-cli".into(), "kafka-cli".into()],
            ca_struct: vec![
                sub_struct_expected.clone(),
                sub_struct_expected.clone(),
                sub_struct_expected.clone(),
            ],
        };
        assert_eq!(expected, from_bytes(bytes).unwrap());
    }
}
