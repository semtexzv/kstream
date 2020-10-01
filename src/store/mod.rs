use std::ops::Deref;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use crate::format::Format;
use std::marker::PhantomData;
use crate::{Config, KStream};
use crate::stream::{StreamItem, KSink};
use std::hash::Hash;
use crate::stream::topic::{TypedProducer, RawConsumer, RawProducer, TypedConsumer};
use std::collections::hash_map::Entry;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    global: Config,
    name: String,
    partiton: i32,
}

#[async_trait(? Send)]
pub trait KVStore<K, V> {
    async fn new(cfg: StoreConfig) -> Self where Self: Sized;
    async fn poll(&mut self);
    async fn get<'a>(&'a mut self, key: &K) -> Option<&'a V>;
    async fn put(&mut self, key: K, value: V) -> Option<V>;
    async fn delete(&mut self, key: &K) -> Option<V>;
}


pub struct Materialized<KF: Format, VF: Format, S: KVStore<KF::Item, VF::Item>> {
    store: S,
    part: i32,
    consumer: TypedConsumer<KF, VF>,
    producer: TypedProducer<KF, VF>,
}

#[async_trait(? Send)]
impl<KF: Format, VF: Format, S: KVStore<KF::Item, VF::Item>> KVStore<KF::Item, VF::Item> for Materialized<KF, VF, S> {
    async fn new(cfg: StoreConfig) -> Self where Self: Sized {
        trace!("Creating materialized table from changelog : {:?}", cfg);

        let mut changelog = crate::stream::topic::TypedConsumer::<KF, VF> {
            raw: RawConsumer::new(&cfg.global.clone().set_group("__UNMANAGED"), &cfg.name, false),
            _marker: PhantomData,
        };

        changelog.raw.assign_partition(cfg.partiton);
        let (_, until) = changelog.raw.offset_range(cfg.partiton);
        let mut last = 0;

        let desired_part = cfg.partiton;
        let mut store = S::new(cfg.clone()).await;

        trace!("Loading changelog until {:?}", until);
        // Load all available messages and rebuild state from them
        while last < until {
            match changelog.next().await {
                StreamItem::Rebalance(p) => {
                    panic!("Changelog topics can't be externally rebalanced")
                }
                // TODO: Implement handling nulls for deletion
                StreamItem::Item(part, k, v) => {
                    assert_eq!(part, desired_part, "Partition changed at runtime");
                    if let Some(v) = v {
                        store.put(k, v).await;
                    } else {
                        store.delete(&k).await;
                    }

                    // TODO: This is so wrong, it'll never work - change it
                    last += 1;
                }
            }
        }
        trace!("Finished loading changelog");
        return Self {
            store,
            part: desired_part,
            consumer: changelog,
            producer: TypedProducer {
                raw: RawProducer::new(&cfg.global, &cfg.name),
                _marker: PhantomData,
            },
        };
    }

    async fn poll(&mut self) {
        match self.consumer.next().await {
            StreamItem::Rebalance(_) => {
                panic!("Rebalance shouldn't occur on changelog topics");
            }
            StreamItem::Item(part, k, v) => {
                if let Some(v) = v {
                    self.put(k, v).await;
                } else {
                    self.delete(&k).await;
                }
            }
        }
        self.store.poll().await;
    }

    async fn get<'a>(&'a mut self, key: &KF::Item) -> Option<&'a VF::Item> {
        self.store.get(key).await
    }

    async fn put(&mut self, key: KF::Item, value: VF::Item) -> Option<VF::Item> {
        // TODO: Fix emitting of messages here
        self.producer.raw.send::<KF, VF>(Some(self.part), &key, Some(&value)).await;
        self.store.put(key, value).await
    }

    async fn delete(&mut self, key: &KF::Item) -> Option<VF::Item> {
        self.producer.send_next(Some(self.part), key, None).await;
        self.store.delete(key).await
    }
}


pub struct InMemory<K: Hash, V> {
    inner: HashMap<K, V>
}

#[async_trait(? Send)]
impl<K: Hash + Eq, V> KVStore<K, V> for InMemory<K, V> {
    async fn new(cfg: StoreConfig) -> Self where Self: Sized {
        return Self {
            inner: HashMap::new()
        };
    }

    async fn poll(&mut self) {}


    async fn get<'a>(&'a mut self, key: &K) -> Option<&'a V> {
        self.inner.get(key)
    }

    async fn put(&mut self, key: K, value: V) -> Option<V>{
        self.inner.insert(key, value)
    }

    async fn delete(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }
}

pub struct Partitioned<S, K, V> {
    name: String,
    global: Config,
    stores: HashMap<i32, S>,
    _mark: PhantomData<(K, V)>,
}

impl<S, K, V> Partitioned<S, K, V>
    where S: KVStore<K, V>
{
    pub fn new(cfg: Config, name: String) -> Self {
        Partitioned {
            name,
            global: cfg,
            stores: HashMap::new(),
            _mark: PhantomData,
        }
    }

    pub async fn ensure_partitions(&mut self, parts: HashSet<i32>) {
        let old = self.stores.keys().map(|v| *v).collect::<HashSet<_>>();
        let added = parts.difference(&old);
        let removed = old.difference(&parts);
        self.repartition(added.map(|v| *v).collect(), removed.map(|v| *v).collect()).await;
    }

    pub async fn repartition(&mut self, added: Vec<i32>, removed: Vec<i32>) {
        for new in added {
            self.stores.insert(new, S::new(StoreConfig {
                global: self.global.clone(),
                name: self.name.clone(),
                partiton: new,
            }).await);
        }
        for old in removed {
            if let Some(v) = self.stores.remove(&old) {
                // Decomission the store
            }
        }
    }

    pub async fn get<'a>(&'a mut self, part: i32, key: &K) -> Option<&'a V> {
        if let Some(v) = self.stores.get_mut(&part) {
            v.get(key).await
        } else {
            None
        }
    }

    pub async fn put(&mut self, part: i32, key: K, value: V) -> Option<V> {
        // TODO: a lot of copying
        let global = self.global.clone();
        let name = self.name.clone();
        match self.stores.entry(part) {
            Entry::Vacant(v) => {
                v.insert(S::new(StoreConfig {
                    global,
                    partiton: part,
                    name,
                }).await);
            }
            _ => {}
        };

        self.stores.get_mut(&part).unwrap().put(key, value).await
    }
    pub async fn delete(&mut self, part: i32, key: &K) -> Option<V> {
        if let Some(p) = self.stores.get_mut(&part) {
            p.delete(key).await
        } else {
            None
        }
    }
}

