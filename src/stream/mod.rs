use std::future::Future;
use rdkafka::Message;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::BaseRecord;
use bytes::Buf;
use rdkafka::consumer::{Consumer, ConsumerContext};

use crate::format::Format;
use crate::{Config, KVStore};

use crate::stream::join::{Join};

use crate::table::TableImpl;
use crate::stream::grouped::Grouped;

pub mod topic;
pub mod join;
pub mod grouped;

pub enum StreamItem<K, V> {
    Rebalance(Vec<i32>),
    Item(i32, K, Option<V>),
}

/// KStream represents a stream of records
/// Each record having a timestamp
#[async_trait(? Send)]
pub trait KStream {
    type Key;
    type Value;

    async fn next(&mut self) -> StreamItem<Self::Key, Self::Value>;
}

pub type BoxedStream<K, V> = Box<dyn KStream<Key=K, Value=V>>;

#[async_trait(? Send)]
pub trait KSink {
    type Key;
    type Value;

    async fn send_next(&mut self, part: Option<i32>, k: &Self::Key, v: Option<&Self::Value>);
}


pub struct Filter<S, F>
    where S: KStream,
          F: Fn(&S::Key, &S::Value) -> bool
{
    pub(crate) stream: S,
    pub(crate) filt: F,
}

#[async_trait(? Send)]
impl<S, F> KStream for Filter<S, F>
    where S: KStream,
          F: Fn(&S::Key, &S::Value) -> bool
{
    type Key = S::Key;
    type Value = S::Value;

    async fn next(&mut self) -> StreamItem<Self::Key, Self::Value> {
        loop {
            match self.stream.next().await {
                // Pass nulls as they are, they could be deletes
                StreamItem::Item(p, k, Some(v)) => {
                    if (self.filt)(&k, &v) {
                        return StreamItem::Item(p, k, Some(v));
                    }
                }

                x => return x,
            };
        }
    }
}


pub struct Map<S, V, F>
    where
        S: KStream,
        F: Fn(&S::Key, S::Value) -> V
{
    pub(crate) stream: S,
    pub(crate) mapper: F,
}


#[async_trait(? Send)]
impl<S, F, V> KStream for Map<S, V, F>
    where
        S: KStream,
        F: Fn(&S::Key, S::Value) -> V
{
    type Key = S::Key;
    type Value = V;

    async fn next(&mut self) -> StreamItem<Self::Key, Self::Value> {
        match self.stream.next().await {
            StreamItem::Rebalance(r) => {
                return StreamItem::Rebalance(r);
            }
            StreamItem::Item(part, k, v) => {
                info!("Map item");
                let v = v.map(|v| (self.mapper)(&k, v));
                return StreamItem::Item(part, k, v);
            }
        }
    }
}




