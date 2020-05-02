use rdkafka::producer::{BaseRecord, BaseProducer, FutureProducer, FutureRecord};
use crate::stream::{KSink, Change};
use crate::format::Format;
use crate::{KStream, Config};
use std::marker::PhantomData;
use rdkafka::consumer::{ConsumerContext, Rebalance, Consumer, BaseConsumer, StreamConsumer};
use rdkafka::{TopicPartitionList, ClientContext, Offset};
use rdkafka::client::NativeClient;
use rdkafka::config::{FromClientConfig, FromClientConfigAndContext};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use rdkafka::message::Message;
use bytes::{Buf, Bytes};
use futures::StreamExt;
use std::time::Duration;
use std::sync::Arc;

struct Context {
    rebalance_changed: AtomicBool
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(parts) => {
                self.rebalance_changed.store(true, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}


pub struct RawConsumer {
    topic: String,
    base: Arc<BaseConsumer<Context>>,
}

impl RawConsumer {
    pub fn new(config: &Config, name: &str, sub: bool) -> Self {
        let context = Context { rebalance_changed: Default::default() };
        let consumer = BaseConsumer::from_config_and_context(&config.0, context).unwrap();
        if sub {
            consumer.subscribe(&[name]).unwrap();
        }
        Self {
            topic: name.to_string(),
            base: Arc::new(consumer),
        }
    }

    pub fn offset_range(&mut self, part: i32) -> (i64, i64) {
        let md = self.base.fetch_watermarks(&self.topic, part, None).unwrap();
        return md;
    }

    pub fn assign_partition(&mut self, part: i32) {
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(&self.topic, part, Offset::Beginning);
        self.base.assign(&list).unwrap();
    }

    pub async fn poll_next<KF: Format, VF: Format>(&mut self) -> Change<KF::Item, VF::Item> {
        loop {
            let rebalanced = self.base.get_base_consumer().context().rebalance_changed.load(Ordering::SeqCst);
            if rebalanced {
                let list = self.base.assignment();
                let parts = list.unwrap().elements().into_iter().map(|p| {
                    if p.topic() != self.topic {
                        panic!("Assigned wrong topic")
                    }
                    p.partition()
                }).collect();
                self.base.get_base_consumer().context().rebalance_changed.store(false, Ordering::SeqCst);
                return Change::Rebalance(parts);
            }
            if let Some(msg) = self.base.poll(Duration::from_millis(1)) {
                match msg {
                    Ok(message) => {
                        let key = KF::deserialize(message.key().unwrap());
                        let val = message.payload().map(VF::deserialize);
                        return Change::Item(message.partition(), key, val);
                    }
                    e => {
                        panic!("e {:?}", e);
                    }
                }
            } else {
                let _ = tokio::time::timeout(Duration::from_millis(1), futures::future::pending::<()>()).await;
            }
        }
    }
}

pub struct TypedConsumer<KF, VF> {
    pub(crate) raw: RawConsumer,
    pub(crate) _marker: PhantomData<(KF, VF)>,
}

#[async_trait(? Send)]
impl<KF, VF> KStream for TypedConsumer<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    async fn next(&mut self) -> Change<Self::Key, Self::Value> {
        self.raw.poll_next::<KF, VF>().await
    }
}


pub struct RawProducer {
    topic: String,
    base: FutureProducer,
}

impl RawProducer {
    pub fn new(config: &Config, topic: &str) -> Self {
        let base = FutureProducer::from_config(&config.0).unwrap();
        RawProducer {
            topic: topic.to_string(),
            base,
        }
    }

    pub async fn send<KF: Format, VF: Format>(&mut self, part: Option<i32>, k: &KF::Item, v: Option<&VF::Item>) {
        let mut rec = FutureRecord::to(&self.topic);
        let k = KF::serialize(&k);
        let v = v.map(|v| VF::serialize(&v));
        rec.key = Some(k.bytes());
        rec.payload = v.as_deref();
        rec.partition = part;

        self.base.send(rec, 100).await.unwrap().unwrap();
    }
}


pub struct TypedProducer<KF, VF> {
    pub(crate) raw: RawProducer,
    pub(crate) _marker: PhantomData<(KF, VF)>,
}

impl<VF, KF> From<RawProducer> for TypedProducer<VF, KF> {
    fn from(p: RawProducer) -> Self {
        return Self {
            raw: p,
            _marker: PhantomData,
        };
    }
}

#[async_trait(? Send)]
impl<KF, VF> KSink for TypedProducer<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    async fn send_next(&mut self, part: Option<i32>, k: &Self::Key, v: Option<&Self::Value>) {
        self.raw.send::<KF, VF>(part, k, v).await;
    }
}


/*
pub struct Topic<KF, VF> {
    name: String,
    consumer: BaseConsumer<Context>,
    consumer_offset: Option<i64>,

    producer: rdkafka::producer::BaseProducer,
    _marker: PhantomData<(KF, VF)>,
}

impl<KF, VF> Topic<KF, VF>
    where KF: Format, VF: Format
{
    pub fn named(config: &Config, name: &str) -> Self
    {
        let context = Context { rebalance_changed: Default::default() };
        let consumer = BaseConsumer::from_config_and_context(&config.0, context).unwrap();
        consumer.subscribe(&[name]).unwrap();
        Topic {
            name: name.to_string(),
            consumer,
            consumer_offset: None,
            producer: rdkafka::producer::BaseProducer::from_config(&config.0).unwrap(),
            _marker: PhantomData,
        }
    }

    pub fn max_offset(&mut self, part: i32) -> i64 {
        let md = self.consumer.fetch_watermarks(&self.name, part, None).unwrap();
        return md.1;
    }

    pub fn set_partition(&mut self, part: i32) {
        let mut list = TopicPartitionList::new();
        list.add_partition_offset(&self.name, part, Offset::Beginning);
        self.consumer.assign(&list).unwrap();
    }
    pub fn next_item(&self) -> Change<KF::Item, VF::Item> {
        loop {
            let rebalanced = self.consumer.context().rebalance_changed.load(Ordering::SeqCst);
            if rebalanced {
                let list = self.consumer.assignment();
                let parts = list.unwrap().elements().into_iter().map(|p| {
                    if p.topic() != self.name {
                        panic!("Assigned wrong topic")
                    }
                    p.partition()
                }).collect();
                self.consumer.context().rebalance_changed.store(false, Ordering::SeqCst);
                return Change::Rebalance(parts);
            }
            for message in self.consumer.iter() {
                match message {
                    Ok(message) => {
                        let key = KF::deserialize(message.key().unwrap());
                        let val = VF::deserialize(message.payload().unwrap());
                        return Change::Item(message.partition(), key, val);
                    }
                    Err(e) => {
                        panic!("e {:?}", e);
                    }
                }
            }
        }
    }
    pub fn send_raw(&mut self, part: Option<i32>, k: Bytes, v: Option<Bytes>) {
        info!("Sink raw");

        let mut rec = BaseRecord::to(&self.name)
            .key(k.bytes());
        rec.payload = v.as_deref();
        rec.partition = part;

        if let Ok(()) = self.producer.send(rec) {
            self.producer.poll(rdkafka::util::Timeout::Never);
            self.producer.flush(rdkafka::util::Timeout::Never);
        }
    }
}

impl<KF, VF> KStream for Topic<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    fn next(&mut self) -> Change<KF::Item, VF::Item> {
        self.next_item()
    }
}

impl<KF, VF> KSink for Topic<KF, VF>
    where KF: Format, VF: Format
{
    type Key = KF::Item;
    type Value = VF::Item;

    fn send_next(&mut self, k: Self::Key, v: Self::Value) {
        self.send_raw(None, KF::serialize(&k), Some(VF::serialize(&v)));
    }
}
*/