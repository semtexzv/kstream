use crate::{KStream, Config, KVStore, KTable};
use crate::format::Format;
use crate::stream::topic::{RawConsumer, TypedConsumer, TypedProducer, RawProducer};
use crate::stream::{StreamItem, KSink, Map, Filter};
use serde::export::PhantomData;

use crate::table::{TableImpl, Change};
use crate::stream::grouped::{Grouped, AggregateTable, Aggregate};
use crate::store::Partitioned;


// A way to add names to existing stores / streams
pub trait Named<T> {
    fn with_name(&mut self, name: String) -> T;
}



/// Task is a base unit of computation,
/// Each task constitutes a consumer group.
/// Each task executes within a single thread, across multiple servers, and internally manages
/// Partitioning. It is a DAG of the complete computation graph.
pub struct Task<S> {
    cfg: Config,
    name: String,
    _stream: S,
}

impl Task<()> {
    pub fn new(cfg: Config, name: &str) -> Self {
        return Self {
            _stream: (),
            cfg,
            name: name.to_string(),
        };
    }
}

impl Task<()> {
    pub fn stream<KF: Format, VF: Format>(self, topic: &str) -> Task<TypedConsumer::<KF, VF>> {
        let cfg = self.cfg.clone().set_group((self.name.clone() + topic).as_ref());
        Task {
            _stream: TypedConsumer::<KF, VF> {
                raw: RawConsumer::new(&cfg, topic, true),
                _marker: PhantomData,
            },
            cfg: self.cfg,
            name: self.name,
        }
    }
}

impl<S: KStream> Task<S> {
    pub fn map<V, F: Fn(&S::Key, S::Value) -> V>(self, mapper: F) -> Task<Map<S, V, F>> {
        Task {
            _stream: Map {
                stream: self._stream,
                mapper: mapper,
            },
            cfg: self.cfg,
            name: self.name,
        }
    }
    pub fn filter<F: Fn(&S::Key, &S::Value) -> bool>(self, filt: F) -> Task<Filter<S, F>> {
        Task {
            _stream: Filter {
                stream: self._stream,
                filt: filt,
            },
            cfg: self.cfg,
            name: self.name,
        }
    }

    pub fn table<ST>(self, name: &str) -> Task<TableImpl<S, ST>>
        where ST: KVStore<S::Key, S::Value>
    {
        Task {
            _stream: TableImpl::new(self._stream, self.cfg.clone(), name.to_string()),
            name: self.name,
            cfg: self.cfg,
        }
    }
    pub fn group(self) -> Task<Grouped<S>> {
        return Task {
            _stream: Grouped {
                stream: self._stream,
            },
            name: self.name,
            cfg: self.cfg,
        };
    }

     pub async fn to<KF, VF>(mut self, name: &str) -> !
        where KF: Format<Item=S::Key>,
              VF: Format<Item=S::Value>
    {
        let mut sink = TypedProducer::<KF, VF> {
            raw: RawProducer::new(&self.cfg, name),
            _marker: PhantomData,
        };
        info!("Sink enter");
        loop {
            match self._stream.next().await {
                StreamItem::Rebalance(_) => {
                    error!("Rebalancing in sink");
                }
                StreamItem::Item(part, key, val) => {
                    info!("Sinking item");
                    sink.send_next(Some(part), &key, val.as_ref()).await;
                }
            }
        }
    }
}

impl<T> Task<T>
    where T: KTable
{
    pub async fn poll(mut self) -> ! {
        loop {
            let _ = self._stream.next().await;
            //self._stream.poll().await;
        }
    }

    pub async fn to_todo<KF, VF>(mut self, name: &str) -> !
        where KF: Format<Item=T::Key>,
              VF: Format<Item=T::Value>
    {
        let mut sink = TypedProducer::<KF, VF> {
            raw: RawProducer::new(&self.cfg, name),
            _marker: PhantomData,
        };
        info!("Sink enter");
        loop {
            match self._stream.next().await {
                (k, Change::New(v)) => {
                    info!("New item");
                    sink.send_next(None, &k, Some(&v)).await
                }
                (k, Change::Modified(_, v)) => {
                    info!("Modified");
                    sink.send_next(None, &k, Some(&v)).await;
                }
                (k, Change::Removed(_)) => {
                    info!("Removed");
                    sink.send_next(None, &k, None).await;
                }
            }
        }
    }
}

impl<S: KStream> Task<Grouped<S>> {
    pub fn aggregate<AGG, KF, AF, ST>(self, agg: AGG) -> Task<AggregateTable<S, AGG, KF, AF, ST>>
        where AGG: Aggregate<S::Key, S::Value>,
              KF: Format<Item=S::Key>,
              AF: Format<Item=AGG::Item>,
              ST: KVStore<S::Key, AGG::Item>
    {
        Task {
            cfg: self.cfg.clone(),
            _stream: AggregateTable {
                stream: self._stream.stream,
                inner: Partitioned::new(self.cfg, "TODO".to_string()),
                agg,
            },
            name: self.name,

        }
    }
}