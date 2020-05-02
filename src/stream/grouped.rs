use crate::{KStream, KTable, KVStore, Config};
use crate::table::BoxedTable;
use serde::Serialize;
use crate::format::json::JSON;
use serde::de::DeserializeOwned;
use crate::format::Format;
use crate::store::{Materialized, InMemory, StoreConfig, Partitioned};
use std::hash::Hash;
use crate::stream::Change;

pub struct Grouped<S> {
    pub(crate) stream: S
}

impl<S: KStream> Grouped<S> {
    pub fn agg<AGG, KF, AF>(self, cfg: Config, agg: AGG) -> GroupedTable<S, AGG, KF, AF, InMemory<KF::Item, AF::Item>>
        where S::Key: Eq + Hash,
              KF: Format<Item=S::Key> + Eq,
              AF: Format<Item=AGG::Item>,
              AGG: Aggregate<S::Key, S::Value>,
    {
        return GroupedTable {
            inner: Partitioned::new(cfg, "TODO".to_string()),
            stream: self.stream,
        };
    }
}

pub trait Aggregate<K, V> {
    type Item;
    fn init() -> Self::Item;
    fn add(old: &Self::Item, k: &K, v: V) -> Self::Item;
    fn remove(old: &Self::Item, k: &K) -> Self::Item;
}

pub struct Count {}

impl<K: Eq, V> Aggregate<K, V> for Count {
    type Item = u64;

    fn init() -> Self::Item {
        0
    }

    fn add(old: &Self::Item, k: &K, v: V) -> Self::Item {
        old + 1
    }
    fn remove(old: &Self::Item, k: &K) -> Self::Item {
        old - 1
    }
}

pub struct GroupedTable<S, AGG, KF, AF, ST>
    where
        S: KStream,
        KF: Format<Item=S::Key>,
        AF: Format<Item=AGG::Item>,
        AGG: Aggregate<S::Key, S::Value>,
        ST: KVStore<KF::Item, AGG::Item>
{
    pub(crate) stream: S,
    pub(crate) inner: Partitioned<Materialized<KF, AF, ST>, KF::Item, AGG::Item>,
}

#[async_trait(?Send)]
impl<S, AGG, KF, AF, ST> KTable for GroupedTable<S, AGG, KF, AF, ST>
    where S::Key: Eq,
          S: KStream,
          KF: Format<Item=S::Key>,
          AF: Format<Item=AGG::Item>,
          AGG: Aggregate<S::Key, S::Value>,
          ST: KVStore<KF::Item, AGG::Item>
{
    type Key = S::Key;
    type Value = AGG::Item;

    async fn poll(&mut self) {
        loop {
            match self.stream.next().await {
                Change::Rebalance(p) => {
                    self.inner.ensure_partitions(p.into_iter().collect()).await;
                }
                Change::Item(p, k, v) => {
                    if let Some(v) = v {
                        let init = AGG::init();
                        let old = self.inner.get(p, &k).unwrap_or(&init);
                        let new = AGG::add(old, &k, v);
                        self.inner.put(p, k, new);
                    } else {
                        if let Some(old) = self.inner.get(p, &k) {
                            let new = AGG::remove(old, &k);
                            self.inner.put(p, k, new);
                        }
                    }
                }
            }
        }
    }
}