// use serde::ser::{Serialize, SerializeSeq, Serializer};
//
// #[derive(Debug, PartialEq, Eq)]
// pub struct SimpleArray<T>(pub Vec<T>);
//
// // impl<T: Serialize> Serialize for SimpleKafkaArray<T> {
// //     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// //     where
// //         S: Serializer,
// //     {
// //         // Just serialize the elements, no length
// //         for item in &self.0 {
// //             item.serialize(serializer)?;
// //         }
// //         serializer.end()
// //     }
// // }
//
// impl<T> Serialize for SimpleArray<T>
// where
//     T: Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
//         for e in &self.0 {
//             seq.serialize_element(e)?;
//         }
//         seq.end()
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use super::*;
//     use serde_test::{assert_tokens, Token};
//
//     #[test]
//     fn test_ser_de() {
//         let array = SimpleArray(vec![1, 2, 3]);
//         assert_tokens(
//             &array,
//             &[
//                 Token::Tuple { len: 3 },
//                 Token::I32(20),
//                 Token::I32(10),
//                 Token::I32(30),
//                 Token::MapEnd,
//             ],
//         );
//     }
// }
