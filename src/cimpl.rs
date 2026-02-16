use crate::StoredValue;
use bytes::Bytes;
use std::collections::{HashMap, LinkedList};
use std::time::Instant;

pub trait StartFrom<A> {
    fn pop(ll: &mut LinkedList<A>) -> Option<A>;
}

pub struct Right {}

impl<A> StartFrom<A> for Right {
    fn pop(ll: &mut LinkedList<A>) -> Option<A> {
        ll.pop_back()
    }
}

pub struct Left;

impl<A> StartFrom<A> for Left {
    fn pop(ll: &mut LinkedList<A>) -> Option<A> {
        ll.pop_front()
    }
}

#[inline]
pub fn insert(
    hm: &mut HashMap<Bytes, StoredValue>,
    key: &[u8],
    value: &[u8],
    will_live_until: Option<Instant>,
) {
    let value = Bytes::copy_from_slice(value);
    hm.insert(
        Bytes::copy_from_slice(key),
        match will_live_until {
            None => StoredValue::Plain(value),
            Some(instant) => StoredValue::TtlPlain(value, instant),
        },
    );
}

pub enum Pop {
    None,
    Multiple(Vec<Bytes>),
    Single(Bytes),
}

pub fn pop<A: StartFrom<Bytes>>(ll: &mut LinkedList<Bytes>, maybe_count: Option<usize>) -> Pop {
    let is_multi_count = maybe_count.is_some();
    let mut times_to_pop = maybe_count.unwrap_or(0);
    let mut to_ret = Vec::new();
    while let Some(v) = A::pop(ll)
        && times_to_pop > 0
    {
        to_ret.push(v.clone());
        times_to_pop -= 1;
    }
    if is_multi_count {
        Pop::Multiple(to_ret)
    } else if !to_ret.is_empty() {
        Pop::Single(to_ret[0].clone())
    } else {
        Pop::None
    }
}
