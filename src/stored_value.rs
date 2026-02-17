use bytes::Bytes;
use std::collections::{HashMap, HashSet, LinkedList};
use std::time::Instant;

#[derive(Debug)]
pub enum StoredValue {
    Plain(Bytes),
    TtlPlain(Bytes, Instant),
    List(LinkedList<Bytes>),
    Dict(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
}
