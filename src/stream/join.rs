use crate::{KStream, KVStore};

pub enum JoinType {
    Inner,
    Left,
    Outer,
}

// Semantics, await for event on LEFT + RIGHT with window specified so we don't overstep
// If we get result, update states & return event
// Next time, The windows have changed & we repeat
pub struct Join<L: KStream, R: KStream> {
    pub(crate) left: L,
    pub(crate) right: R,
    pub(crate) join_type: JoinType,
    pub(crate) store: Box<dyn KVStore<L::Key, (Option<L::Value>, Option<R::Value>)>>,
    pub(crate) windows: (u64, u64),
}
