use std::ops::Deref;

pub trait KVStore<K, V> {
    fn get(&self, key: &K) -> Option<&V>;
    fn put(&mut self, key: K, value: V);
    fn delete(&mut self, key: K) -> Option<V>;
}