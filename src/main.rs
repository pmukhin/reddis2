mod cmd;
mod dict_ops;
mod err;
mod hmap_ops;
mod list_ops;
mod memory;
mod numerical_ops;
mod ops;
mod set_ops;
mod sorted_set_ops;
mod stats;
mod stored_value;

use crate::cmd::Command;
use crate::dict_ops::HMapDictOps;
use crate::err::RedisError;
use hmap_ops::HMapOps;

use crate::list_ops::{HMapListOps, Popped};
use crate::memory::memory_usage;
use crate::numerical_ops::HMapNumericalOps;
use crate::set_ops::HMapSetOps;
use crate::sorted_set_ops::HMapSortedSetOps;
use crate::stored_value::StoredValue;
use anyhow::Context;
use bytes::Bytes;
use compact_str::CompactString;
use histogram::Histogram;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tracing::{info, trace, warn};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const SERVER: Token = Token(0);

struct Client {
    ops: ops::Ops,
    read_buf: Vec<u8>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let uptime_since = Instant::now();

    let mut hmap: HashMap<Bytes, StoredValue> = HashMap::default();
    let _ttl_map: BTreeMap<Instant, Bytes> = BTreeMap::default();

    let addr = "127.0.0.1:6379".parse()?;
    let mut listener = TcpListener::bind(addr)?;

    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(128);

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    let mut clients: HashMap<Token, Client> = HashMap::new();
    let mut next_token_id: usize = 1;
    let mut buf = [0u8; 1024];

    let mut latency_histograms = HashMap::<CompactString, Histogram>::new();

    info!("TCP server listening on {addr}");

    loop {
        poll.poll(&mut events, None)?;

        for event in &events {
            match event.token() {
                // New connection coming in
                SERVER => loop {
                    let (mut stream, addr) = match listener.accept() {
                        Ok(accepted) => accepted,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => anyhow::bail!(e),
                    };
                    let token = Token(next_token_id);
                    next_token_id += 1;

                    trace!("[{token:?}] Connected: {addr}");

                    poll.registry().register(
                        &mut stream,
                        token,
                        Interest::READABLE | Interest::WRITABLE,
                    )?;

                    clients.insert(
                        token,
                        Client {
                            ops: ops::Ops::new(stream),
                            read_buf: Vec::with_capacity(4096),
                        },
                    );
                },

                // Activity on an existing client connection
                token => {
                    let mut closed = false;
                    // for huge responses
                    let mut to_return = Vec::<u8>::new();

                    let cmd_instant = Instant::now();
                    let current_command: CompactString;

                    let clients_len = clients.len();
                    let client = clients
                        .get_mut(&token)
                        .with_context(|| format!("client not registered: {:?}", token))?;

                    if event.is_readable() {
                        loop {
                            let cmd = match client.ops.read(&mut buf) {
                                Ok(0) => {
                                    // Connection closed by peer
                                    closed = true;
                                    break;
                                }
                                Ok(n) => {
                                    client.read_buf.extend_from_slice(&buf[..n]);
                                    let maybe_command = cmd::parser::parse(&client.read_buf);
                                    match maybe_command {
                                        Err(RedisError::IncompleteInput) => continue,
                                        Err(err) => {
                                            client.ops.generic_error(err.to_string())?;
                                            break;
                                        }
                                        Ok(command) => command,
                                    }
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    eprintln!("[{token:?}] Read error: {e}");
                                    closed = true;
                                    break;
                                }
                            };
                            match cmd {
                                Command::Get(key) => {
                                    match hmap.get(key) {
                                        None => client.ops.key_not_found()?,
                                        Some(StoredValue::Plain(bytes)) => {
                                            client.ops.write_bulk_string(bytes)?;
                                        }
                                        Some(_) => client.ops.wrong_type("expected STRING")?,
                                    };
                                    current_command = cmd::GET;
                                }
                                Command::Set(key, value, maybe_ttl) => {
                                    hmap.insert_alloc(
                                        key,
                                        value,
                                        maybe_ttl.map(|dur| Instant::now() + dur),
                                    );
                                    client.ops.ok()?;
                                    current_command = cmd::SET;
                                }
                                Command::SetNx(key, value) => {
                                    hmap.set_if_not_exist(key, value);
                                    client.ops.ok()?;
                                    current_command = cmd::SET;
                                }
                                Command::SetXx(key, value) => {
                                    hmap.update_if_exist(key, value);
                                    client.ops.ok()?;
                                    current_command = cmd::SET;
                                }
                                Command::SetAndGet(key, value) => {
                                    match hmap.insert_alloc(key, value, None) {
                                        None => {
                                            client.ops.key_not_found()?;
                                        }
                                        Some(StoredValue::Plain(bytes)) => {
                                            client.ops.write_bulk_string(&bytes)?;
                                        }
                                        Some(StoredValue::TtlPlain(bytes, _)) => {
                                            client.ops.write_bulk_string(bytes)?;
                                        }
                                        _ => client.ops.wrong_type("expected STRING")?,
                                    }
                                    current_command = cmd::SET;
                                }
                                Command::SetKeepTtl(key, value) => {
                                    match hmap.get_mut(key) {
                                        None => {
                                            hmap.insert_alloc(key, value, None);
                                            client.ops.ok()?;
                                        }
                                        Some(StoredValue::Plain(bytes)) => {
                                            client.ops.write_bulk_string(&bytes)?;
                                            *bytes = Bytes::copy_from_slice(bytes);
                                        }
                                        Some(StoredValue::TtlPlain(bytes, _)) => {
                                            client.ops.write_bulk_string(&bytes)?;
                                            *bytes = Bytes::copy_from_slice(bytes);
                                        }
                                        _ => client.ops.wrong_type("expected STRING")?,
                                    };
                                    current_command = cmd::SET;
                                }
                                Command::Ping => {
                                    client.ops.pong()?;
                                    current_command = cmd::PING;
                                }
                                Command::FlushDb => {
                                    client.ops.ok()?;
                                    current_command = cmd::FLUSHDB;
                                }
                                Command::Docs => {
                                    client.ops.write_array(std::iter::empty::<&[u8]>(), 0)?;
                                    current_command = cmd::DOCS;
                                }
                                Command::DbSize => {
                                    client.ops.write_integer(hmap.keys().len())?;
                                    current_command = cmd::DBSIZE;
                                }
                                Command::Config => {
                                    client.ops.write_array(std::iter::empty::<&[u8]>(), 0)?;
                                    current_command = cmd::CONFIG;
                                }
                                Command::Lpush(key, values) => {
                                    match hmap.prepend(key, values) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(values_len) => client.ops.write_integer(values_len)?,
                                    };
                                    current_command = cmd::LPUSH;
                                }
                                Command::Rpush(key, values) => {
                                    match hmap.append(key, values) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(values_len) => client.ops.write_integer(values_len)?,
                                    };
                                    current_command = cmd::RPUSH;
                                }
                                Command::LpushX(_, _) => {
                                    todo!("Command::LpushX is not implemented")
                                }
                                Command::RpushX(_, _) => {
                                    todo!("Command::RpushX is not implemented")
                                }
                                Command::Lpop(key, maybe_count) => {
                                    match hmap.pop_front(key, maybe_count) {
                                        Ok(Popped::None) => client.ops.key_not_found()?,
                                        Ok(Popped::Single(bytes)) => {
                                            client.ops.write_bulk_string(bytes)?
                                        }
                                        Ok(Popped::Multiple(values)) => {
                                            client.ops.write_array(values.iter(), values.len())?
                                        }
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                    }
                                    current_command = cmd::LPOP;
                                }
                                Command::Rpop(key, maybe_count) => {
                                    match hmap.pop_back(key, maybe_count) {
                                        Ok(Popped::None) => client.ops.key_not_found()?,
                                        Ok(Popped::Single(bytes)) => {
                                            client.ops.write_bulk_string(bytes)?
                                        }
                                        Ok(Popped::Multiple(values)) => {
                                            client.ops.write_array(values.iter(), values.len())?
                                        }
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                    }
                                    current_command = cmd::RPOP;
                                }
                                Command::Del(keys) => {
                                    let count = hmap.delete_all(keys.into_iter());
                                    client.ops.write_integer(count)?;
                                    current_command = cmd::DEL;
                                }
                                Command::Incr(key) => {
                                    match hmap.incr_by(key, 1) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(value)) => client
                                            .ops
                                            .write_integer(String::from_utf8_lossy(&value))?,
                                    };
                                    current_command = cmd::INCR;
                                }
                                Command::IncrBy(key, incr_by) => {
                                    match hmap.incr_by(key, incr_by) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(value)) => client
                                            .ops
                                            .write_integer(String::from_utf8_lossy(&value))?,
                                    };
                                    current_command = cmd::INCR;
                                }
                                Command::ClientSetInfo(_) | Command::ClientSetName => {
                                    client.ops.ok()?;
                                    current_command = cmd::CLIENT;
                                }
                                Command::Ttl(key) => {
                                    match hmap.get_ttl(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(value)) => {
                                            client.ops.write_integer(value.as_secs())?
                                        }
                                    };
                                    current_command = cmd::TTL;
                                }
                                Command::Lrange(key, start, end) => {
                                    match hmap.get(key) {
                                        None => client.ops.key_not_found()?,
                                        Some(StoredValue::List(ll)) => {
                                            let start = if start < 0 {
                                                ll.len() as isize - start
                                            } else {
                                                start
                                            };
                                            let end = if end < 0 {
                                                ll.len() as isize - end
                                            } else {
                                                end
                                            };
                                            // @todo optimise this...
                                            info!(
                                                "lpop: key = {}, start = {}, end = {}, ll={:?}",
                                                String::from_utf8_lossy(key),
                                                start,
                                                end,
                                                &ll
                                            );

                                            let vec: Vec<_> = if start <= end {
                                                ll.iter()
                                                    .skip(start as usize)
                                                    .take(end as usize + 1)
                                                    .collect()
                                            } else {
                                                ll.iter()
                                                    .skip(end as usize)
                                                    .take(start as usize + 1)
                                                    .rev()
                                                    .collect()
                                            };
                                            client.ops.write_array(vec.iter(), vec.len())?;
                                        }
                                        _ => client.ops.wrong_type("stored value isn't a list")?,
                                    };
                                    current_command = cmd::LRANGE;
                                }
                                Command::LLen(key) => {
                                    match hmap.get(key) {
                                        None => client.ops.key_not_found()?,
                                        Some(StoredValue::List(ll)) => {
                                            client.ops.write_integer(ll.len())?;
                                        }
                                        _ => client.ops.wrong_type("stored value isn't a list")?,
                                    };
                                    current_command = cmd::LLEN;
                                }
                                Command::Hget(key, field) => {
                                    match hmap.dict_get(key, field) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(value)) => client.ops.write_bulk_string(value)?,
                                    };
                                    current_command = cmd::HGET;
                                }
                                Command::HMget(key, fields) => {
                                    match hmap.dict_mget(key, &fields) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((values, len))) => {
                                            client.ops.write_array(values.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::HMGET;
                                }
                                Command::HMset(key, fields_and_values) => {
                                    match hmap.dict_mset(key, &fields_and_values) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(()) => client.ops.ok()?,
                                    }
                                    current_command = cmd::HMSET;
                                }
                                Command::HgetAll(key) => {
                                    match hmap.dict_get_all(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((values, len))) => {
                                            client.ops.write_array(values.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::HMSET;
                                }
                                Command::HincrBy(key, field, incr_by) => {
                                    match hmap.dict_incr_by(key, field, incr_by) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(value) => client
                                            .ops
                                            .write_integer(String::from_utf8_lossy(&value))?,
                                    }
                                    current_command = cmd::HINCRBY;
                                }
                                Command::Exists(key) => {
                                    let exists = if hmap.contains_key(key) { 1 } else { 0 };
                                    client.ops.write_integer(exists)?;
                                    current_command = cmd::EXISTS;
                                }
                                Command::Hexists(key, field) => {
                                    match hmap.dict_exists(key, field) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(exists) => {
                                            let v = if exists { 1 } else { 0 };
                                            client.ops.write_integer(v)?;
                                        }
                                    }
                                    current_command = cmd::HEXISTS;
                                }
                                Command::Hkeys(key) => {
                                    match hmap.dict_keys(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((keys, len))) => {
                                            client.ops.write_array(keys.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::HKEYS;
                                }
                                Command::Sadd(key, members) => {
                                    match hmap.set_add(key, members) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(added) => client.ops.write_integer(added)?,
                                    };
                                    current_command = cmd::SADD;
                                }
                                Command::Sismember(key, member) => {
                                    match hmap.set_is_member(key, member) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(exists) => {
                                            let v = if exists { 1 } else { 0 };
                                            client.ops.write_integer(v)?;
                                        }
                                    }
                                    current_command = cmd::SISMEMBER;
                                }
                                Command::Sinter(keys) => {
                                    match hmap.set_inter(&keys) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok((values, len)) => {
                                            client.ops.write_array(values.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::SINTER
                                }
                                Command::Sunion(keys) => {
                                    match hmap.set_union(&keys) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok((values, len)) => {
                                            client.ops.write_array(values.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::SUNION;
                                }
                                Command::Sdiff(keys) => {
                                    match hmap.set_diff(&keys) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok((values, len)) => {
                                            client.ops.write_array(values.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::SDIFF;
                                }
                                Command::Scard(key) => {
                                    match hmap.set_card(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(len)) => client.ops.write_integer(len)?,
                                    };
                                    current_command = cmd::SCARD;
                                }
                                Command::Smembers(key) => {
                                    match hmap.set_members(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((members, len))) => {
                                            client.ops.write_array(members.into_iter(), len)?
                                        }
                                    };
                                    current_command = cmd::SMEMBERS;
                                }
                                Command::Zadd(key, members) => {
                                    match hmap.zset_add(key, &members) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(added) => client.ops.write_integer(added)?,
                                    };
                                    current_command = cmd::ZADD;
                                }
                                Command::Zrange(key, start, stop, withscores) => {
                                    match hmap.zset_range(key, start, stop, withscores) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((values, len))) => {
                                            client.ops.write_array(values.iter(), len)?
                                        }
                                    };
                                    current_command = cmd::ZRANGE;
                                }
                                Command::Zrevrange(key, start, stop, withscores) => {
                                    match hmap.zset_revrange(key, start, stop, withscores) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((values, len))) => {
                                            client.ops.write_array(values.iter(), len)?
                                        }
                                    };
                                    current_command = cmd::ZREVRANGE;
                                }
                                Command::Zrank(key, member) => {
                                    match hmap.zset_rank(key, member) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(rank)) => client.ops.write_integer(rank)?,
                                    };
                                    current_command = cmd::ZRANK;
                                }
                                Command::Zrevrank(key, member) => {
                                    match hmap.zset_revrank(key, member) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(rank)) => client.ops.write_integer(rank)?,
                                    };
                                    current_command = cmd::ZREVRANK;
                                }
                                Command::Zscore(key, member) => {
                                    match hmap.zset_score(key, member) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(score)) => {
                                            client.ops.write_bulk_string(score.to_string())?
                                        }
                                    };
                                    current_command = cmd::ZSCORE;
                                }
                                Command::Zrangebyscore(key, min, max, withscores) => {
                                    match hmap.zset_range_by_score(key, min, max, withscores) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some((values, len))) => {
                                            client.ops.write_array(values.iter(), len)?
                                        }
                                    };
                                    current_command = cmd::ZRANGEBYSCORE;
                                }
                                Command::Zincrby(key, incr, member) => {
                                    match hmap.zset_incr_by(key, incr, member) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(score) => {
                                            client.ops.write_bulk_string(score.to_string())?
                                        }
                                    };
                                    current_command = cmd::ZINCRBY;
                                }
                                Command::Zcard(key) => {
                                    match hmap.zcard(key) {
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                        Ok(None) => client.ops.key_not_found()?,
                                        Ok(Some(value)) => client.ops.write_integer(value)?,
                                    };
                                    current_command = cmd::ZCARD;
                                }
                                Command::InfoCmd => {
                                    let (alloc, _) = memory_usage()?;
                                    let alloc_readable =
                                        format!("{}K", f64::trunc(alloc as f64 / 1024.0));
                                    let uptime_in_seconds = uptime_since.elapsed().as_secs();
                                    let uptime_in_days = uptime_since.elapsed().as_secs() / 24;
                                    let command_stats =
                                        stats::CommandStats::make(&latency_histograms);

                                    let info = format!(
                                        "# Server\r\n\
                                         redis_version:reddis2-0.0.1\r\n\
                                         redis_mode:standalone\r\n\
                                         os:Rust/mio\r\n\
                                         arch_bits:64\r\n\
                                         tcp_port:6379\r\n\
                                         uptime_in_seconds:{uptime_in_seconds}\r\n\
                                         uptime_in_days:{uptime_in_days}\r\n\
                                         hz:10\r\n\
                                         executable:/usr/local/bin/reddis2\r\n\
                                         config_file:\r\n\
                                         \r\n\
                                         # Clients\r\n\
                                         connected_clients:{clients_len}\r\n\
                                         blocked_clients:0\r\n\
                                         tracking_clients:0\r\n\
                                         maxclients:10000\r\n\
                                         \r\n\
                                         # Memory\r\n\
                                         used_memory:{alloc}\r\n\
                                         used_memory_human:{alloc_readable}\r\n\
                                         used_memory_peak:2048000\r\n\
                                         maxmemory:0\r\n\
                                         maxmemory_human:0B\r\n\
                                         maxmemory_policy:noeviction\r\n\
                                         mem_fragmentation_ratio:2.00\r\n\
                                         \r\n\
                                         # Stats\r\n\
                                         total_connections_received:100\r\n\
                                         total_commands_processed:1337\r\n\
                                         instantaneous_ops_per_sec:42\r\n\
                                         rejected_connections:0\r\n\
                                         expired_keys:0\r\n\
                                         evicted_keys:0\r\n\
                                         keyspace_hits:500\r\n\
                                         keyspace_misses:50\r\n\
                                         \r\n\
                                         # Replication\r\n\
                                         role:master\r\n\
                                         connected_slaves:0\r\n\
                                         \r\n\
                                         # CPU\r\n\
                                         used_cpu_sys:0.420000\r\n\
                                         used_cpu_user:0.690000\r\n\
                                         \r\n\
                                         {command_stats}\
                                         \r\n\
                                         # Keyspace\r\n\
                                         db0:keys={},expires=0,avg_ttl=0\r\n",
                                        hmap.len(),
                                    );
                                    client.ops.write_bulk_string(&info)?;
                                    current_command = cmd::INFO;
                                }
                                Command::LatencyHistogram(commands) => {
                                    client
                                        .ops
                                        .write_latency_histogram(&latency_histograms, &commands)?;
                                    current_command = cmd::LATENCY;
                                }
                            }
                            client.read_buf.clear();

                            let latency = cmd_instant.elapsed().as_micros() as u64;
                            latency_histograms
                                .entry(current_command.clone())
                                .or_insert(Histogram::new(2, 30)?)
                                .increment(latency)
                                .with_context(|| {
                                    format!("can't store latency {latency} for {current_command}")
                                })?;

                            trace!(
                                "[{token:?}] command is executed, buffer cleared, latency: {}usecs",
                                latency
                            );
                            break;
                        }
                    }

                    if event.is_writable() && !to_return.is_empty() {
                        match client.ops.ok() {
                            Ok(()) => {
                                trace!("[{token:?}] Echoed {} bytes", to_return.len());
                                to_return.clear();
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                warn!("[{token:?}] Write error: {e}");
                                closed = true;
                            }
                        }
                    }

                    if closed && let Some(client) = clients.remove(&token) {
                        poll.registry()
                            .deregister(&mut client.ops.unwrap_stream())?;
                        trace!("[{token:?}] disconnected");
                    }
                }
            }
        }
    }
}
