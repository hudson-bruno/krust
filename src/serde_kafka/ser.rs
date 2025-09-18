use std::num::TryFromIntError;

use bytes::{BufMut, BytesMut};
use serde::{ser, Serialize};
use tokio::io::AsyncWriteExt;

use crate::serde_kafka::COMPACT_STRING_NAME;

use super::error::{Error, Result};

pub struct Serializer {
    output: BytesMut,
}

pub fn to_bytes_mut<T>(value: &T) -> Result<BytesMut>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: BytesMut::new(),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

pub async fn to_async_writer_with_message_size<W, S>(writer: &mut W, value: &S) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    S: Serialize,
{
    let response_bytes = to_bytes_mut(value)?;
    let mut result = BytesMut::new();
    result.extend_from_slice(&(response_bytes.len() as i32).to_be_bytes());
    result.extend_from_slice(&response_bytes);

    writer.write_all_buf(&mut result).await.unwrap();
    writer.flush().await.unwrap();
    Ok(())
}

impl ser::Serializer for &mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.put_u8(v as u8);
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.output.put_i8(v);
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.output.put_i16(v);
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.output.put_i32(v);
        Ok(())
    }

    fn serialize_i64(self, _v: i64) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.output.put_u8(v);
        Ok(())
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.output.put_u32(v);
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        unimplemented!()
    }

    fn serialize_f32(self, _v: f32) -> Result<()> {
        unimplemented!()
    }

    fn serialize_f64(self, _v: f64) -> Result<()> {
        unimplemented!()
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        unimplemented!()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        let length: i16 = v
            .len()
            .try_into()
            .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

        self.output.put_i16(length);
        self.output.put_slice(v.as_bytes());

        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let length: i32 = v
            .len()
            .try_into()
            .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

        self.output.put_i32(length);
        self.output.put_slice(v);

        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        unimplemented!()
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if name == COMPACT_STRING_NAME {
            let s = value.serialize(StringCapture)?;

            let length: i8 = s
                .len()
                .try_into()
                .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

            self.output.put_i8(length + 1);
            self.output.put_slice(s.as_bytes());

            Ok(())
        } else {
            value.serialize(self)
        }
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let length: i8 = len
            .ok_or(Error::Message("Size must be known".into()))?
            .try_into()
            .map_err(|e: TryFromIntError| Error::Message(e.to_string()))?;

        self.output.put_i8(length + 1);
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        unimplemented!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }
}

impl ser::SerializeSeq for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl ser::SerializeTupleVariant for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl ser::SerializeMap for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl ser::SerializeStruct for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}
struct StringCapture;

macro_rules! reject_other_types {
    ($self:ident, $ty:expr) => {
        Err(Error::Message(format!(
            "CompactString can only contain a string, not {}",
            $ty
        )))
    };
}

impl ser::Serializer for StringCapture {
    type Ok = String;

    type Error = Error;

    type SerializeSeq = ser::Impossible<String, Error>;

    type SerializeTuple = ser::Impossible<String, Error>;

    type SerializeTupleStruct = ser::Impossible<String, Error>;

    type SerializeTupleVariant = ser::Impossible<String, Error>;

    type SerializeMap = ser::Impossible<String, Error>;

    type SerializeStruct = ser::Impossible<String, Error>;

    type SerializeStructVariant = ser::Impossible<String, Error>;

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        Ok(v.to_string())
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok> {
        reject_other_types!(self, "bool")
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok> {
        reject_other_types!(self, "i8")
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok> {
        reject_other_types!(self, "i16")
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok> {
        reject_other_types!(self, "i32")
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok> {
        reject_other_types!(self, "i64")
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok> {
        reject_other_types!(self, "u8")
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok> {
        reject_other_types!(self, "u16")
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok> {
        reject_other_types!(self, "u32")
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok> {
        reject_other_types!(self, "u64")
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok> {
        reject_other_types!(self, "f32")
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok> {
        reject_other_types!(self, "f64")
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok> {
        reject_other_types!(self, "bytes")
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        reject_other_types!(self, "none")
    }

    fn serialize_some<T: ?Sized + Serialize>(self, _v: &T) -> Result<Self::Ok> {
        reject_other_types!(self, "some")
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        reject_other_types!(self, "unit")
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        reject_other_types!(self, "unit_struct")
    }

    fn serialize_unit_variant(
        self,
        _n: &'static str,
        _vi: u32,
        _var: &'static str,
    ) -> Result<Self::Ok> {
        reject_other_types!(self, "unit_variant")
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _n: &'static str,
        v: &T,
    ) -> Result<Self::Ok> {
        v.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _n: &'static str,
        _vi: u32,
        _var: &'static str,
        _v: &T,
    ) -> Result<Self::Ok> {
        reject_other_types!(self, "newtype_variant")
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        reject_other_types!(self, "seq")
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        reject_other_types!(self, "tuple")
    }

    fn serialize_tuple_struct(
        self,
        _n: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        reject_other_types!(self, "tuple_struct")
    }

    fn serialize_tuple_variant(
        self,
        _n: &'static str,
        _vi: u32,
        _var: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        reject_other_types!(self, "tuple_variant")
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        reject_other_types!(self, "map")
    }

    fn serialize_struct(self, _n: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        reject_other_types!(self, "struct")
    }

    fn serialize_struct_variant(
        self,
        _n: &'static str,
        _vi: u32,
        _var: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        reject_other_types!(self, "struct_variant")
    }
}
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use crate::serde_kafka::CompactString;

    use super::*;

    #[derive(Serialize, Debug, PartialEq)]
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
        ca_compact_string: Vec<CompactString>,
        ca_struct: Vec<SubStruct>,
    }

    #[derive(Serialize, PartialEq, Debug, Clone)]
    struct SubStruct {
        i8: i8,
        i16: i16,
        i32: i32,
    }

    #[test]
    fn test_struct_serialization() {
        let sub_struct_expected = SubStruct {
            i8: i8::MAX,
            i16: i16::MAX,
            i32: i32::MAX,
        };
        let value = Test {
            i8: i8::MAX,
            i16: i16::MAX,
            i32: i32::MAX,
            string: "kafka-cli".into(),
            compact_string: CompactString("hello".into()),
            ca_i8: vec![i8::MAX, i8::MAX, i8::MAX],
            ca_i16: vec![i16::MAX, i16::MAX, i16::MAX],
            ca_i32: vec![i32::MAX, i32::MAX, i32::MAX],
            ca_string: vec!["kafka-cli".into(), "kafka-cli".into(), "kafka-cli".into()],
            ca_compact_string: vec!["kafka-cli".into(), "kafka-cli".into(), "kafka-cli".into()],
            ca_struct: vec![
                sub_struct_expected.clone(),
                sub_struct_expected.clone(),
                sub_struct_expected.clone(),
            ],
        };

        let mut expected = BytesMut::new();

        // i8: 127
        expected.put_i8(i8::MAX);
        // i16: 32767
        expected.put_i16(i16::MAX);
        // i32: 2147483647
        expected.put_i32(i32::MAX);
        // string: "kafka-cli" (len 9)
        expected.put_i16(9);
        expected.put_slice(b"kafka-cli");
        // compact_string: "hello" (len 5, so writes 6 as i8)
        expected.put_i8(6);
        expected.put_slice(b"hello");
        // ca_i8: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        expected.put_i8(i8::MAX);
        expected.put_i8(i8::MAX);
        expected.put_i8(i8::MAX);
        // ca_i16: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        expected.put_i16(i16::MAX);
        expected.put_i16(i16::MAX);
        expected.put_i16(i16::MAX);
        // ca_i32: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        expected.put_i32(i32::MAX);
        expected.put_i32(i32::MAX);
        expected.put_i32(i32::MAX);
        // ca_string: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        expected.put_i16(9);
        expected.put_slice(b"kafka-cli");
        expected.put_i16(9);
        expected.put_slice(b"kafka-cli");
        expected.put_i16(9);
        expected.put_slice(b"kafka-cli");
        // ca_compact_string: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        expected.put_i8(10);
        expected.put_slice(b"kafka-cli");
        expected.put_i8(10);
        expected.put_slice(b"kafka-cli");
        expected.put_i8(10);
        expected.put_slice(b"kafka-cli");
        // ca_struct: vec![1, 2, 3] (len 3 as i8)
        expected.put_i8(4);
        // i8: 127
        expected.put_i8(i8::MAX);
        // i16: 32767
        expected.put_i16(i16::MAX);
        // i32: 2147483647
        expected.put_i32(i32::MAX);
        // i8: 127
        expected.put_i8(i8::MAX);
        // i16: 32767
        expected.put_i16(i16::MAX);
        // i32: 2147483647
        expected.put_i32(i32::MAX);
        // i8: 127
        expected.put_i8(i8::MAX);
        // i16: 32767
        expected.put_i16(i16::MAX);
        // i32: 2147483647
        expected.put_i32(i32::MAX);

        let result = to_bytes_mut(&value).unwrap();

        assert_eq!(result, expected);
    }
}
