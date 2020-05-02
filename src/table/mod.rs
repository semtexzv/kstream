use crate::{KStream, Config};
use crate::store::{KVStore, Partitioned};
use crate::stream::Change;
use std::collections::HashSet;
use std::iter::FromIterator;

#[async_trait(?Send)]
pub trait KTable {
    type Key;
    type Value;

    async fn poll(&mut self);
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

#[async_trait(?Send)]
impl<S, ST> KTable for TableImpl<S, ST>
    where S: KStream,
          ST: KVStore<S::Key, S::Value>
{
    type Key = S::Key;
    type Value = S::Value;

    async fn poll(&mut self) {
        loop {
            match self.stream.next().await {
                Change::Rebalance(p) => {
                    let new = HashSet::from_iter(p.into_iter());
                    let removed = self.balance.difference(&new);
                    let added = new.difference(&self.balance);
                    trace!("msg in table");
                    self.store.repartition(added.map(|v| *v).collect(), removed.map(|v| *v).collect()).await;
                }
                Change::Item(part, k, v) => {
                    if let Some(v) = v {
                        trace!("Insert");
                        self.store.put(part, k, v)
                    } else {
                        trace!("Delete");
                        self.store.delete(part, &k);
                    }
                }
            }
        }
    }
}