use std::marker::PhantomData;

pub trait KTable {
    type Key;
    type Value;
}