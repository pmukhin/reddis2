use std::fmt::Debug;
use std::time::Duration;

pub mod parser;

use compact_str::CompactString;

pub const GET: CompactString = CompactString::const_new("get");
pub const SET: CompactString = CompactString::const_new("set");
pub const PING: CompactString = CompactString::const_new("ping");
pub const FLUSHDB: CompactString = CompactString::const_new("flushdb");
pub const DOCS: CompactString = CompactString::const_new("docs");
pub const DBSIZE: CompactString = CompactString::const_new("dbsize");
pub const CONFIG: CompactString = CompactString::const_new("config");
pub const LPUSH: CompactString = CompactString::const_new("lpush");
pub const RPUSH: CompactString = CompactString::const_new("rpush");
pub const LPOP: CompactString = CompactString::const_new("lpop");
pub const RPOP: CompactString = CompactString::const_new("rpop");
pub const DEL: CompactString = CompactString::const_new("del");
pub const INCR: CompactString = CompactString::const_new("incr");
pub const CLIENT: CompactString = CompactString::const_new("client");
pub const TTL: CompactString = CompactString::const_new("ttl");
pub const LRANGE: CompactString = CompactString::const_new("lrange");
pub const LLEN: CompactString = CompactString::const_new("llen");
pub const HGET: CompactString = CompactString::const_new("hget");
pub const HMGET: CompactString = CompactString::const_new("hmget");
pub const HMSET: CompactString = CompactString::const_new("hmset");
pub const HINCRBY: CompactString = CompactString::const_new("hincrby");
pub const EXISTS: CompactString = CompactString::const_new("exists");
pub const HEXISTS: CompactString = CompactString::const_new("hexists");
pub const HKEYS: CompactString = CompactString::const_new("hkeys");
pub const SADD: CompactString = CompactString::const_new("sadd");
pub const SISMEMBER: CompactString = CompactString::const_new("sismember");
pub const SINTER: CompactString = CompactString::const_new("sinter");
pub const SUNION: CompactString = CompactString::const_new("sunion");
pub const SDIFF: CompactString = CompactString::const_new("sdiff");
pub const SCARD: CompactString = CompactString::const_new("scard");
pub const SMEMBERS: CompactString = CompactString::const_new("smembers");
pub const ZADD: CompactString = CompactString::const_new("zadd");
pub const ZRANGE: CompactString = CompactString::const_new("zrange");
pub const ZREVRANGE: CompactString = CompactString::const_new("zrevrange");
pub const ZRANK: CompactString = CompactString::const_new("zrank");
pub const ZREVRANK: CompactString = CompactString::const_new("zrevrank");
pub const ZSCORE: CompactString = CompactString::const_new("zscore");
pub const ZRANGEBYSCORE: CompactString = CompactString::const_new("zrangebyscore");
pub const ZINCRBY: CompactString = CompactString::const_new("zincrby");
pub const ZCARD: CompactString = CompactString::const_new("zcard");
pub const INFO: CompactString = CompactString::const_new("info");
pub const LATENCY: CompactString = CompactString::const_new("latency");

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
    ClientSetName,
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
    LatencyHistogram(Vec<&'a [u8]>),
}
