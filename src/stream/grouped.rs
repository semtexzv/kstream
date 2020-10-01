use crate::{KStream, KTable, KVStore, Config};
use crate::table::{BoxedTable, Change, TableImpl};
use serde::Serialize;
use crate::format::json::JSON;
use serde::de::DeserializeOwned;
use crate::format::Format;
use crate::store::{Materialized, InMemory, StoreConfig, Partitioned};
use std::hash::Hash;
use crate::stream::StreamItem;
use serde::export::PhantomData;

pub struct Grouped<S> {
    pub(crate) stream: S
}

impl<S: KStream> Grouped<S> {
    pub fn agg<AGG, KF, AF>(self, cfg: Config, agg: AGG) -> AggregateTable<S, AGG, KF, AF, InMemory<KF::Item, AF::Item>>
        where S::Key: Eq + Hash,
              KF: Format<Item=S::Key> + Eq,
              AF: Format<Item=AGG::Item>,
              AGG: Aggregate<S::Key, S::Value>,
    {
        return AggregateTable {
            inner: Partitioned::new(cfg, "TODO".to_string()),
            stream: self.stream,
            agg,
        };
    }
}

pub trait Aggregate<K, V> {
    type Item: Clone;
    fn init(&self, k: &K, v: &V) -> Self::Item;
    fn add(&self, old: Self::Item, k: &K, v: &V) -> Self::Item;
}

pub struct Count {}

impl<K: Eq, V> Aggregate<K, V> for Count {
    type Item = u64;

    fn init(&self, k: &K, v: &V) -> Self::Item {
        1
    }

    fn add(&self, old: Self::Item, k: &K, v: &V) -> Self::Item {
        old + 1
    }
}

pub struct ArgMin<ST, VT, SelF> {
    select: SelF,
    _marker: PhantomData<(ST, VT)>,
}

impl<ST: Ord, VT, SelF> ArgMin<ST, VT, SelF>
{
    pub fn new<K, V>(select: SelF) -> Self
        where SelF: Fn(&K, &V) -> (ST, VT),
    {
        Self {
            select,
            _marker: PhantomData,
        }
    }
}

impl<ST: Ord, VT, SelF, K, V, > Aggregate<K, V> for ArgMin<ST, VT, SelF>
    where SelF: Fn(&K, &V) -> (ST, VT),
          ST: Clone,
          VT: Clone
{
    type Item = (ST, VT);

    fn init(&self, k: &K, v: &V) -> Self::Item {
        (self.select)(k, v)
    }

    fn add(&self, mut old: Self::Item, k: &K, v: &V) -> Self::Item {
        let (sel, val) = old;
        let (new_sel, new_val) = (self.select)(&k, &v);
        if new_sel < sel {
            return (new_sel, new_val);
        } else {
            return (sel, val);
        }
    }
}

/// Table which considers each of i
pub struct AggregateTable<S, AGG, KF, AF, ST>
    where
        S: KStream,
        KF: Format<Item=S::Key>,
        AF: Format<Item=AGG::Item>,
        AGG: Aggregate<S::Key, S::Value>,
        ST: KVStore<KF::Item, AGG::Item>
{
    pub(crate) stream: S,
    pub(crate) inner: Partitioned<Materialized<KF, AF, ST>, KF::Item, AGG::Item>,
    pub(crate) agg: AGG,
}

#[async_trait(? Send)]
impl<S, AGG, KF, AF, ST> KTable for AggregateTable<S, AGG, KF, AF, ST>
    where S: KStream,
          S::Key: Eq + Clone,
          AGG::Item: Clone,
          KF: Format<Item=S::Key>,
          AF: Format<Item=AGG::Item>,
          AGG: Aggregate<S::Key, S::Value>,
          ST: KVStore<KF::Item, AGG::Item>
{
    type Key = S::Key;
    type Value = AGG::Item;

    async fn next(&mut self) -> (Self::Key, Change<Self::Value>) {
        loop {
            match self.stream.next().await {
                StreamItem::Rebalance(p) => {
                    trace!("Rebalancing grouped");
                    self.inner.ensure_partitions(p.into_iter().collect()).await;
                }
                StreamItem::Item(p, k, v) => {
                    trace!("Grouped item");
                    if let Some(v) = v {
                        trace!("Add agg");
                        let old = self.inner.delete(p, &k).await;
                        if let Some(old) = old {
                            let new = AGG::add(&self.agg, old.clone(), &k, &v);
                            self.inner.put(p, k.clone(), new.clone()).await;
                            return (k, Change::Modified(old, new));
                        } else {
                            let new = AGG::init(&self.agg, &k, &v);
                            self.inner.put(p, k.clone(), new.clone()).await;
                            return (k, Change::New(new));
                        }
                    }
                }
            }
        }
    }
}