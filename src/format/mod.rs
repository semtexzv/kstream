use bytes::Bytes;
use std::marker::PhantomData;

pub mod json;

use json::JSON;

pub trait Format {
    type Item;
    fn serialize(v: &Self::Item) -> Bytes {
        unimplemented!()
    }
    fn deserialize(v: &[u8]) -> Self::Item {
        unimplemented!()
    }
}

pub fn json<T>() -> JSON<T> {
    JSON(PhantomData)
}
