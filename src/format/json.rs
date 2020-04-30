use std::marker::PhantomData;
use bytes::Bytes;

use crate::format::Format;

#[derive(Debug)]
pub struct JSON<T>(pub(crate) PhantomData<T>);

impl<T: Default> Format for JSON<T> {
    type Item = T;
}