mod compact_string;
mod de;
mod error;
mod ser;
mod simple_seq;
pub mod uuid_as_bytes;

pub use de::{
    from_async_reader_trail_with_message_size, from_async_reader_with_message_size, from_bytes,
    Deserializer,
};
pub use error::{Error, Result};
pub use ser::{to_async_writer_with_message_size, to_bytes_mut, Serializer};

pub use compact_string::*;
pub use simple_seq::*;
