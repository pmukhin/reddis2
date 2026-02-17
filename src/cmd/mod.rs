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
    HMget(&'a [u8], Vec<&'a [u8]>),
    HMset(&'a [u8], Vec<&'a [u8]>),
    HgetAll(&'a [u8]),
    HincrBy(&'a [u8], &'a [u8], i64),
    Exists(&'a [u8]),
    Hexists(&'a [u8], &'a [u8]),
    Hkeys(&'a [u8]),
    Sadd(&'a [u8], Vec<&'a [u8]>),
    Sismember(&'a [u8], &'a [u8]),
    Sinter(Vec<&'a [u8]>),
    Sunion(Vec<&'a [u8]>),
    Sdiff(Vec<&'a [u8]>),
    Scard(&'a [u8]),
    Smembers(&'a [u8]),
    Zadd(&'a [u8], Vec<(i64, &'a [u8])>),
    Zrange(&'a [u8], isize, isize, bool),
    Zrevrange(&'a [u8], isize, isize, bool),
    Zrank(&'a [u8], &'a [u8]),
    Zrevrank(&'a [u8], &'a [u8]),
    Zscore(&'a [u8], &'a [u8]),
    Zrangebyscore(&'a [u8], i64, i64, bool),
    Zincrby(&'a [u8], i64, &'a [u8]),
    Zcard(&'a [u8]),
    InfoCmd,
    LatencyHistogram,
}
