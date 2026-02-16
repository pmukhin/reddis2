use std::fmt::Debug;
use std::time::Duration;

pub mod parser;

#[derive(Debug, PartialEq, Eq)]
pub enum Info<'a> {
    LibName(&'a [u8]),
    LibVersion(&'a [u8]),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Ping,
    Docs,
    DbSize,
    Config,
    Get(&'a [u8]),
    Set(&'a [u8], &'a [u8], Option<Duration>),
    SetNx(&'a [u8], &'a [u8]),
    SetXx(&'a [u8], &'a [u8]),
    SetAndGet(&'a [u8], &'a [u8]),
    SetKeepTtl(&'a [u8], &'a [u8]),
    Lpush(&'a [u8], Vec<&'a [u8]>),
    Rpush(&'a [u8], Vec<&'a [u8]>),
    LpushX(&'a [u8], Vec<&'a [u8]>),
    RpushX(&'a [u8], Vec<&'a [u8]>),
    Lpop(&'a [u8], Option<usize>),
    Rpop(&'a [u8], Option<usize>),
    Lrange(&'a [u8], isize, isize),
    Del(Vec<&'a [u8]>),
    Incr(&'a [u8]),
    IncrBy(&'a [u8], i64),
    FlushDb,
    ClientSetInfo(Info<'a>),
    Ttl(&'a [u8]),
    LLen(&'a [u8]),
    Hget(&'a [u8], &'a [u8]),
    Hset(&'a [u8], &'a [u8], &'a [u8]),
    HMget(&'a [u8], Vec<&'a [u8]>),
    HMset(&'a [u8], Vec<&'a [u8]>),
    HgetAll(&'a [u8]),
}
