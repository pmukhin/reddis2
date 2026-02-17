use crate::StoredValue;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::Instant;

pub trait HMapOps<K, V> {
    fn set_if_not_exist(&mut self, key: &[u8], value: &[u8]);

    fn update_if_exist(&mut self, key: &[u8], value: &[u8]);

    fn insert_alloc(
        &mut self,
        key: &[u8],
        value: &[u8],
        maybe_end_of_life: Option<Instant>,
    ) -> Option<StoredValue>;

    fn delete_all<'a>(&'a mut self, keys: impl Iterator<Item = &'a [u8]>);
}

impl HMapOps<Bytes, StoredValue> for HashMap<Bytes, StoredValue> {
    fn set_if_not_exist(&mut self, key: &[u8], value: &[u8]) {
        if !self.contains_key(key) {
            self.insert_alloc(key, value, None);
        }
    }

    fn update_if_exist(&mut self, key: &[u8], value: &[u8]) {
        if self.contains_key(key) {
            self.insert_alloc(key, value, None);
        }
    }

    fn insert_alloc(
        &mut self,
        key: &[u8],
        value: &[u8],
        maybe_end_of_life: Option<Instant>,
    ) -> Option<StoredValue> {
        let value = Bytes::copy_from_slice(value);
        self.insert(
            Bytes::copy_from_slice(key),
            match maybe_end_of_life {
                None => StoredValue::Plain(value),
                Some(instant) => StoredValue::TtlPlain(value, instant),
            },
        )
    }

    fn delete_all<'a>(&'a mut self, keys: impl Iterator<Item = &'a [u8]>) {
        for key in keys {
            self.remove(key);
        }
    }
}
