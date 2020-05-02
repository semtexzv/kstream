use bytes::Bytes;
use std::marker::PhantomData;

pub mod json;

use json::JSON;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub trait Format {
    type Item;
    fn serialize(v: &Self::Item) -> Bytes;
    fn deserialize(v: &[u8]) -> Self::Item;
}

pub fn json<T>() -> JSON<T>
    where T: Serialize + DeserializeOwned {
    JSON(PhantomData)
}
