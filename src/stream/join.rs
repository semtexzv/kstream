use crate::KStream;

pub enum JoinType {
    Inner,
    Left,
    Outer,
}

pub trait JoinTarget {
    type Key;
    type Value;
}

//
// Semantics, await for event on LEFT + RIGHT with window specified so we don't overstep
// If we get result, update states & return event
// Next time, The windows have changed & we repeat
pub struct Join<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
    pub(crate) join_type: JoinType,
}

impl<L, R> KStream for Join<L, R>
    where L: KStream, R: JoinTarget<Key=L::Key>
{
    type Key = L::Key;
    type Value = (L::Value, R::Value);

    fn next(&self) -> (Self::Key, Self::Value) {
        unimplemented!()
    }
}
