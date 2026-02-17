use crate::stored_value::StoredValue;
use anyhow::bail;
use bytes::Bytes;
use std::collections::{HashMap, LinkedList};
use tracing::info;

pub enum Popped {
    None,
    Multiple(Vec<Bytes>),
    Single(Bytes),
}

pub trait HMapListOps {
    fn append(&mut self, key: &[u8], values: Vec<&[u8]>) -> anyhow::Result<usize>;

    fn prepend(&mut self, key: &[u8], values: Vec<&[u8]>) -> anyhow::Result<usize>;

    fn pop_front(&mut self, key: &[u8], n: Option<usize>) -> anyhow::Result<Popped>;

    fn pop_back(&mut self, key: &[u8], n: Option<usize>) -> anyhow::Result<Popped>;
}

impl HMapListOps for HashMap<Bytes, StoredValue> {
    fn append(&mut self, key: &[u8], values: Vec<&[u8]>) -> anyhow::Result<usize> {
        let entry = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::List(LinkedList::new()));
        let values_len = values.len();
        match entry {
            StoredValue::List(l) => {
                for value in values {
                    let value = value.to_vec().into_boxed_slice();
                    l.push_back(Bytes::from(value));
                }
                Ok(values_len)
            }
            _ => bail!("cannot LPUSH to a value that is not a LIST"),
        }
    }

    fn prepend(&mut self, key: &[u8], values: Vec<&[u8]>) -> anyhow::Result<usize> {
        let entry = self
            .entry(Bytes::copy_from_slice(key))
            .or_insert(StoredValue::List(LinkedList::new()));
        let values_len = values.len();
        match entry {
            StoredValue::List(l) => {
                for value in values {
                    l.push_front(Bytes::copy_from_slice(value));
                }
                Ok(values_len)
            }
            _ => bail!("cannot RPUSH to a value that is not a LIST"),
        }
    }

    fn pop_front(&mut self, key: &[u8], n: Option<usize>) -> anyhow::Result<Popped> {
        let ll = match self.get_mut(key) {
            None => return Ok(Popped::None),
            Some(StoredValue::List(ll)) => ll,
            _ => bail!("cannot LPOP from a value that is not a LIST"),
        };
        let is_multi_count = n.is_some();
        let mut times_to_pop = n.unwrap_or(1);
        let mut to_ret = Vec::new();
        while times_to_pop > 0 && let Some(v) = ll.pop_front() {
            to_ret.push(v.clone());
            times_to_pop -= 1;
        }
        Ok(if is_multi_count {
            Popped::Multiple(to_ret)
        } else if !to_ret.is_empty() {
            Popped::Single(to_ret[0].clone())
        } else {
            Popped::None
        })
    }

    fn pop_back(&mut self, key: &[u8], n: Option<usize>) -> anyhow::Result<Popped> {
        let ll = match self.get_mut(key) {
            None => return Ok(Popped::None),
            Some(StoredValue::List(ll)) => ll,
            _ => bail!("cannot LPOP from a value that is not a LIST"),
        };

        let is_multi_count = n.is_some();
        let mut times_to_pop = n.unwrap_or(1);
        let mut to_ret = Vec::new();
        while times_to_pop > 0 && let Some(v) = ll.pop_back()
        {
            to_ret.push(v.clone());
            times_to_pop -= 1;
        }
        Ok(if is_multi_count {
            Popped::Multiple(to_ret)
        } else if !to_ret.is_empty() {
            Popped::Single(to_ret[0].clone())
        } else {
            Popped::None
        })
    }
}
