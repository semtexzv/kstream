use std::future::Future;
use rdkafka::Message;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::BaseRecord;
use bytes::Buf;
use rdkafka::consumer::{Consumer, ConsumerContext};

use crate::format::Format;
use crate::{Config, KVStore};

use crate::stream::join::{Join};
use crate::stream::map::Map;
use crate::table::TableImpl;
use crate::stream::grouped::Grouped;

pub mod topic;
pub mod join;
pub mod map;
pub mod grouped;
pub mod filter;

pub enum Change<K, V> {
    Rebalance(Vec<i32>),
    Item(i32, K, Option<V>),
}

/// KStream represents a stream of records
/// Each record having a timestamp
#[async_trait(?Send)]
pub trait KStream {
    type Key;
    type Value;

    async fn next(&mut self) -> Change<Self::Key, Self::Value>;
}

pub type BoxedStream<K, V> = Box<dyn KStream<Key=K, Value=V>>;

#[async_trait(?Send)]
pub trait KSink {
    type Key;
    type Value;

    async fn send_next(&mut self, part: Option<i32>, k: &Self::Key, v: Option<&Self::Value>);
}


