use crate::{KStream, Config};
use crate::store::{KVStore, Partitioned};
use crate::stream::StreamItem;
use std::collections::HashSet;
use std::iter::FromIterator;

use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Change<V> {
    New(V),
    Modified(V, V),
    Removed(V),
}

impl<V> Change<V> {
    pub fn values(self) -> (Option<V>, Option<V>) {
        match self {
            Change::New(v) => (None, Some(v)),
            Change::Modified(v1, v2) => (Some(v1), Some(v2)),
            Change::Removed(v) => (Some(v), None)
        }
    }
}

#[async_trait(? Send)]
pub trait KTable {
    type Key: Clone;
    type Value: Clone;

    async fn next(&mut self) -> (Self::Key, Change<Self::Value>);
}

pub type BoxedTable<K, V> = Box<dyn KTable<Key=K, Value=V>>;

pub struct TableImpl<S: KStream, ST> {
    stream: S,
    store: Partitioned<ST, S::Key, S::Value>,
    balance: HashSet<i32>,
}

impl<S, ST> TableImpl<S, ST>
    where S: KStream,
          ST: KVStore<S::Key, S::Value>
{
    pub fn new(stream: S, cfg: Config, mut store_name: String) -> Self {
        store_name.push_str("-changelog");
        let mut partitioned = Partitioned::new(cfg, store_name);
        TableImpl {
            stream,
            store: partitioned,
            balance: HashSet::new(),
        }
    }
}

#[async_trait(? Send)]
impl<S, ST> KTable for TableImpl<S, ST>
    where S: KStream,
          S::Key: Clone,
          S::Value: Clone,
          ST: KVStore<S::Key, S::Value>
{
    type Key = S::Key;
    type Value = S::Value;


    async fn next(&mut self) -> (Self::Key, Change<Self::Value>) {
        loop {
            match self.stream.next().await {
                StreamItem::Rebalance(p) => {
                    let new = HashSet::from_iter(p.into_iter());
                    let removed = self.balance.difference(&new);
                    let added = new.difference(&self.balance);
                    trace!("Repartitioning store");
                    self.store.repartition(added.map(|v| *v).collect(), removed.map(|v| *v).collect()).await;
                }
                StreamItem::Item(part, k, v) => {
                    if let Some(v) = v {
                        trace!("Insert");
                        let old = self.store.put(part, k.clone(), v.clone()).await;
                        if let Some(old) = old {
                            return (k, Change::Modified(old, v));
                        } else {
                            return (k, Change::New(v));
                        }
                    } else {
                        trace!("Delete");
                        if let Some(old) = self.store.delete(part, &k).await {
                            return (k, Change::Removed(old));
                        }
                    }
                }
            }
        }
    }
}