use crate::KStream;
use crate::stream::Change;

pub struct Map<S, V, F>
    where
        S: KStream,
        F: Fn(&S::Key, S::Value) -> V
{
    pub(crate) stream: S,
    pub(crate) mapper: F,
}


#[async_trait(?Send)]
impl<S, F, V> KStream for Map<S, V, F>
    where
        S: KStream,
        F: Fn(&S::Key, S::Value) -> V
{
    type Key = S::Key;
    type Value = V;

    async fn next(&mut self) -> Change<Self::Key, Self::Value> {
        match self.stream.next().await {
            Change::Rebalance(r) => {
                return Change::Rebalance(r);
            }
            Change::Item(part, k, v) => {
                info!("Map item");
                let v = v.map(|v| (self.mapper)(&k, v));
                return Change::Item(part, k, v);
            }
        }
    }
}



