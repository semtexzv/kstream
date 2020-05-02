use std::marker::PhantomData;
use bytes::Bytes;

use crate::format::Format;
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug)]
pub struct JSON<T>(pub(crate) PhantomData<T>)
    where T: Serialize + DeserializeOwned;

impl<T> Format for JSON<T>
    where T: Serialize + DeserializeOwned
{
    type Item = T;

    fn serialize(v: &Self::Item) -> Bytes {
        Bytes::from(serde_json::to_vec(v).unwrap())
    }

    fn deserialize(v: &[u8]) -> Self::Item {
        serde_json::from_slice(v).unwrap()
    }
}