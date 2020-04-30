use std::marker::PhantomData;
use std::future::Future;
use rdkafka::Message;
use crate::format::Format;
use crate::Config;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::BaseRecord;
use bytes::Buf;
use crate::stream::join::{Join, JoinTarget};

pub mod join;

/// KStream represents a stream of records
/// Each record having a timestamp
pub trait KStream {
    type Key;
    type Value;

    fn next(&self) -> (Self::Key, Self::Value);
    fn map<K, V, F: Fn(Self::Key, Self::Value) -> (K, V)>(self, mapper: F) -> Map<Self, K, V, F>
        where Self: Sized
    {
        return Map {
            stream: self,
            mapper,
        };
    }
    fn join<J : JoinTarget>(self, join_type: join::JoinType, other: J) -> join::Join<Self, J>
        where Self: Sized
    {
        Join {
            left: self,
            right: other,
            join_type,
        }
    }
    fn sink_to(self, mut sink: impl KSink<Key=Self::Key, Value=Self::Value>)
        where Self: Sized
    {
        loop {
            let (k, v) = self.next();
            sink.send_next(k, v);
        }
    }
}

pub trait KSink {
    type Key;
    type Value;

    fn send_next(&mut self, k: Self::Key, v: Self::Value);
}

pub struct Topic<KF, VF> {
    name: String,
    consumer: rdkafka::consumer::BaseConsumer,
    producer: rdkafka::producer::BaseProducer,
    _marker: PhantomData<(KF, VF)>,
}

impl<KF, VF> Topic<KF, VF> {
    pub fn named(config: &Config, name: &str) -> Self
    {
        Topic {
            name: name.to_string(),
            consumer: rdkafka::consumer::BaseConsumer::from_config(&config.0).unwrap(),
            producer: rdkafka::producer::BaseProducer::from_config(&config.0).unwrap(),
            _marker: PhantomData,
        }
    }
}

impl<KF, VF> KStream for Topic<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    fn next(&self) -> (Self::Key, Self::Value) {
        loop {
            if let Some(message) = self.consumer.poll(rdkafka::util::Timeout::Never) {
                if let Ok(message) = message {
                    let key = KF::deserialize(message.key().unwrap());
                    let val = VF::deserialize(message.payload().unwrap());
                }
            }
        }
    }
}

impl<KF, VF> KSink for Topic<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    fn send_next(&mut self, k: Self::Key, v: Self::Value) {
        if let Ok(()) = self.producer.send(BaseRecord::to(&self.name)
            .key(KF::serialize(&k).bytes())
            .payload(VF::serialize(&v).bytes()))
        {
            self.producer.poll(rdkafka::util::Timeout::Never);
        }
    }
}

pub struct Map<S, K, V, F>
    where
        S: KStream,
        F: Fn(S::Key, S::Value) -> (K, V)
{
    stream: S,
    mapper: F,
}

impl<S, F, K, V> KStream for Map<S, K, V, F>
    where
        S: KStream,
        F: Fn(S::Key, S::Value) -> (K, V)
{
    type Key = K;
    type Value = V;

    fn next(&self) -> (Self::Key, Self::Value) {
        let next = (self.stream.next());
        (self.mapper)(next.0, next.1)
    }
}