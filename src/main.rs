mod cimpl;
mod cmd;
mod err;
mod ops;

use crate::cmd::Command;
use anyhow::Context;
use bytes::Bytes;
use itertools::Itertools;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{BTreeMap, HashMap, LinkedList};
use std::time::Instant;
use tracing::info;
use crate::err::RedisError;

const SERVER: Token = Token(0);

struct Client {
    ops: ops::Ops,
}

#[derive(Debug)]
enum StoredValue {
    Plain(Bytes),
    TtlPlain(Bytes, Instant),
    List(LinkedList<Bytes>),
    Dict(HashMap<Bytes, Bytes>),
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

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

                    info!("[{token:?}] Connected: {addr}");

                    poll.registry().register(
                        &mut stream,
                        token,
                        Interest::READABLE | Interest::WRITABLE,
                    )?;

                    clients.insert(
                        token,
                        Client {
                            ops: ops::Ops::new(stream),
                        },
                    );
                },

                // Activity on an existing client connection
                token => {
                    let mut closed = false;
                    // for huge responses
                    let mut to_return = Vec::<u8>::new();

                    let client = clients
                        .get_mut(&token)
                        .with_context(|| format!("client not registered: {:?}", token))?;

                    if event.is_readable() {
                        let mut buf = [0u8; 1024];
                        let mut total_read = Vec::with_capacity(4096);
                        loop {
                            match client.ops.read(&mut buf) {
                                Ok(0) => {
                                    // Connection closed by peer
                                    closed = true;
                                    break;
                                }
                                Ok(n) => {
                                    total_read.extend_from_slice(&buf[..n]);
                                    match cmd::parser::parse(&total_read) {
                                        Err(RedisError::IncompleteInput) => continue,
                                        Err(err) => {
                                            client.ops.generic_error(err.to_string())?;
                                        }
                                        Ok(Command::Get(key)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Plain(bytes)) => {
                                                client.ops.write_bulk_string(bytes)?;
                                                info!(
                                                    "[{token:?}] GET executed successfully with key {}",
                                                    String::from_utf8_lossy(key)
                                                );
                                            }
                                            Some(value) => {
                                                panic!("expected plain value, got {value:?}")
                                            }
                                        },
                                        Ok(Command::Set(key, value, maybe_ttl)) => {
                                            cimpl::insert(
                                                &mut hmap,
                                                key,
                                                value,
                                                maybe_ttl.map(|dur| Instant::now() + dur),
                                            );
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::SetNx(key, value)) => {
                                            if !hmap.contains_key(key) {
                                                cimpl::insert(&mut hmap, key, value, None);
                                            }
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::SetXx(key, value)) => {
                                            if hmap.contains_key(key) {
                                                cimpl::insert(&mut hmap, key, value, None);
                                            }
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::SetAndGet(key, value)) => {
                                            match hmap.get_mut(key) {
                                                None => {
                                                    cimpl::insert(&mut hmap, key, value, None);
                                                    client.ops.ok()?;
                                                }
                                                Some(StoredValue::Plain(bytes)) => {
                                                    client.ops.write_bulk_string(&bytes)?;
                                                    *bytes = Bytes::copy_from_slice(bytes);
                                                }
                                                Some(StoredValue::TtlPlain(bytes, _)) => {
                                                    client.ops.write_bulk_string(bytes)?;
                                                    cimpl::insert(&mut hmap, key, value, None);
                                                }
                                                _ => client.ops.wrong_type("expected STRING")?,
                                            }
                                        }
                                        Ok(Command::SetKeepTtl(key, value)) => {
                                            match hmap.get_mut(key) {
                                                None => {
                                                    cimpl::insert(&mut hmap, key, value, None);
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
                                            }
                                        }
                                        Ok(Command::Ping) => {
                                            client.ops.pong()?;
                                        }
                                        Ok(Command::FlushDb) => {
                                            info!("FLUSHDB");
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::Docs) => {
                                            println!("Command::CommandDocs")
                                        }
                                        Ok(Command::DbSize) => {
                                            println!("Command::DbSize")
                                        }
                                        Ok(Command::Config) => {
                                            println!("Command::Config")
                                        }
                                        Ok(Command::Lpush(key, values)) => {
                                            let key_allocated: Box<[u8]> =
                                                key.to_vec().into_boxed_slice();
                                            let entry = hmap
                                                .entry(Bytes::from(key_allocated))
                                                .or_insert(StoredValue::List(LinkedList::new()));
                                            let values_len = values.len();
                                            match entry {
                                                StoredValue::List(l) => {
                                                    for value in values {
                                                        let value =
                                                            value.to_vec().into_boxed_slice();
                                                        l.push_front(Bytes::from(value));
                                                    }
                                                    client.ops.write_bulk_string(
                                                        values_len.to_string(),
                                                    )?;
                                                }
                                                _ => client.ops.wrong_type("expected LIST")?,
                                            }
                                        }
                                        Ok(Command::Rpush(key, values)) => {
                                            let entry = hmap
                                                .entry(Bytes::copy_from_slice(key))
                                                .or_insert(StoredValue::List(LinkedList::new()));
                                            let values_len = values.len();
                                            match entry {
                                                StoredValue::List(l) => {
                                                    for value in values {
                                                        let value =
                                                            value.to_vec().into_boxed_slice();
                                                        l.push_back(Bytes::from(value));
                                                    }
                                                    client.ops.write_bulk_string(Bytes::from(
                                                        values_len.to_string(),
                                                    ))?;
                                                }
                                                _ => client.ops.wrong_type("expected LIST")?,
                                            }
                                        }
                                        Ok(Command::LpushX(_, _)) => {
                                            println!("Command::LpushX")
                                        }
                                        Ok(Command::RpushX(_, _)) => {
                                            println!("Command::RpushX")
                                        }
                                        Ok(Command::Lpop(key, maybe_count)) => match hmap
                                            .get_mut(key)
                                        {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::List(ll)) => {
                                                match cimpl::pop::<cimpl::Left>(ll, maybe_count) {
                                                    cimpl::Pop::None => {
                                                        client.ops.key_not_found()?
                                                    }
                                                    cimpl::Pop::Single(value) => {
                                                        client.ops.write_bulk_string(&value)?
                                                    }
                                                    cimpl::Pop::Multiple(values) => {
                                                        client.ops.write_array(values.as_slice())?
                                                    }
                                                }
                                            }
                                            _ => {
                                                client.ops.wrong_type("stored type is not LIST")?
                                            }
                                        },
                                        Ok(Command::Rpop(key, maybe_count)) => match hmap
                                            .get_mut(key)
                                        {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::List(ll)) => {
                                                match cimpl::pop::<cimpl::Right>(ll, maybe_count) {
                                                    cimpl::Pop::None => {
                                                        client.ops.key_not_found()?
                                                    }
                                                    cimpl::Pop::Single(value) => {
                                                        client.ops.write_bulk_string(&value)?
                                                    }
                                                    cimpl::Pop::Multiple(values) => {
                                                        client.ops.write_array(values.as_slice())?
                                                    }
                                                }
                                            }
                                            _ => {
                                                client.ops.wrong_type("stored type is not LIST")?
                                            }
                                        },
                                        Ok(Command::Del(keys)) => {
                                            for key in keys {
                                                hmap.remove(key);
                                            }
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::Incr(key)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Plain(bytes)) => {
                                                let str = String::from_utf8_lossy(bytes);
                                                match str::parse::<i64>(&str) {
                                                    Ok(num) => {
                                                        let new_value = (num + 1).to_string();
                                                        let value_as_bytes = Bytes::copy_from_slice(
                                                            new_value.as_bytes(),
                                                        );
                                                        client
                                                            .ops
                                                            .write_bulk_string(&value_as_bytes)?;
                                                        hmap.insert(
                                                            Bytes::from(
                                                                key.to_vec().into_boxed_slice(),
                                                            ),
                                                            StoredValue::Plain(value_as_bytes),
                                                        );
                                                    }
                                                    Err(_) => client.ops.wrong_type(
                                                        "stored value isn't a 64 bit integer",
                                                    )?,
                                                }
                                            }
                                            Some(value) => {
                                                panic!("expected plain value, got {value:?}")
                                            }
                                        },
                                        Ok(Command::IncrBy(key, incr_by)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Plain(bytes)) => {
                                                let str = String::from_utf8_lossy(bytes);
                                                match str::parse::<i64>(&str) {
                                                    Ok(num) => {
                                                        let new_value = (num + incr_by).to_string();
                                                        let value_as_bytes = Bytes::copy_from_slice(
                                                            new_value.into_bytes().as_slice(),
                                                        );
                                                        client
                                                            .ops
                                                            .write_bulk_string(&value_as_bytes)?;
                                                        hmap.insert(
                                                            Bytes::copy_from_slice(key),
                                                            StoredValue::Plain(value_as_bytes),
                                                        );
                                                    }
                                                    Err(_) => client.ops.wrong_type(
                                                        "stored value isn't a 64 bit integer",
                                                    )?,
                                                }
                                            }
                                            Some(value) => {
                                                panic!("expected plain value, got {value:?}")
                                            }
                                        },
                                        Ok(Command::ClientSetInfo(_)) => {
                                            client.ops.ok()?;
                                        }
                                        Ok(Command::Ttl(key)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::TtlPlain(_bytes, i)) => {
                                                let now = Instant::now();
                                                let diff = i.duration_since(now).as_secs();
                                                client.ops.write_bulk_string(diff.to_string())?;
                                            }
                                            _ => client
                                                .ops
                                                .wrong_type("stored value doesn't have TTL")?,
                                        },
                                        Ok(Command::Lrange(key, start, end)) => {
                                            match hmap.get(key) {
                                                None => client.ops.key_not_found()?,
                                                Some(StoredValue::List(ll)) => {
                                                    let real_start =
                                                        (ll.len() as isize - start) as usize;
                                                    let real_end =
                                                        (ll.len() as isize - end) as usize;
                                                    let vec = ll
                                                        .iter()
                                                        .skip(real_start)
                                                        .take(real_end - real_start)
                                                        .collect::<Vec<_>>();
                                                    client.ops.write_array(vec.as_slice())?;
                                                }
                                                _ => client
                                                    .ops
                                                    .wrong_type("stored value isn't a list")?,
                                            }
                                        }
                                        Ok(Command::LLen(key)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::List(ll)) => {
                                                client
                                                    .ops
                                                    .write_bulk_string(ll.len().to_string())?;
                                            }
                                            _ => client
                                                .ops
                                                .wrong_type("stored value isn't a list")?,
                                        },
                                        Ok(Command::Hget(key, field)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Dict(dict)) => {
                                                match dict.get(field) {
                                                    None => client.ops.key_not_found()?,
                                                    Some(value) => {
                                                        client.ops.write_bulk_string(value)?
                                                    }
                                                }
                                            }
                                            _ => client
                                                .ops
                                                .wrong_type("stored value isn't a dict")?,
                                        },
                                        Ok(Command::Hset(key, field, value)) => {
                                            match hmap.get_mut(key) {
                                                None => {
                                                    let mut dict = HashMap::new();
                                                    dict.insert(
                                                        Bytes::copy_from_slice(field),
                                                        Bytes::copy_from_slice(value),
                                                    );
                                                    hmap.insert(
                                                        Bytes::copy_from_slice(key),
                                                        StoredValue::Dict(dict),
                                                    );
                                                    client.ops.ok()?;
                                                }
                                                Some(StoredValue::Dict(dict)) => {
                                                    dict.insert(
                                                        Bytes::copy_from_slice(field),
                                                        Bytes::copy_from_slice(value),
                                                    );
                                                    client.ops.ok()?;
                                                }
                                                _ => client
                                                    .ops
                                                    .wrong_type("stored value isn't a dict")?,
                                            }
                                        }
                                        Ok(Command::HMget(key, fields)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Dict(dict)) => {
                                                let result = fields
                                                    .iter()
                                                    .filter_map(|f| dict.get(*f))
                                                    .collect::<Vec<_>>();
                                                client.ops.write_array(result.as_slice())?;
                                            }
                                            _ => client
                                                .ops
                                                .wrong_type("stored value isn't a dict")?,
                                        },
                                        Ok(Command::HMset(key, fields_and_values)) => {
                                            let stored_value = hmap
                                                .entry(Bytes::copy_from_slice(key))
                                                .or_insert(StoredValue::Dict(Default::default()));
                                            match stored_value {
                                                StoredValue::Dict(dict) => {
                                                    for mut chunk in
                                                        &fields_and_values.iter().chunks(2)
                                                    {
                                                        let field = chunk
                                                            .next()
                                                            .expect("HMSET is ill-formed");
                                                        let value = chunk
                                                            .next()
                                                            .expect("HMSET is ill-formed");
                                                        dict.insert(
                                                            Bytes::copy_from_slice(field),
                                                            Bytes::copy_from_slice(value),
                                                        );
                                                    }
                                                    client.ops.ok()?;
                                                }
                                                _ => client
                                                    .ops
                                                    .wrong_type("stored value isn't a dict")?,
                                            }
                                        }
                                        Ok(Command::HgetAll(key)) => match hmap.get(key) {
                                            None => client.ops.key_not_found()?,
                                            Some(StoredValue::Dict(dict)) => {
                                                let fields_and_values = dict
                                                    .iter()
                                                    .flat_map(|(k, v)| [k, v])
                                                    .collect::<Vec<_>>();
                                                client
                                                    .ops
                                                    .write_array(fields_and_values.as_slice())?;
                                            }
                                            _ => client
                                                .ops
                                                .wrong_type("stored value isn't a dict")?,
                                        },
                                    }
                                    total_read.clear();
                                    info!("[{token:?}] command is executed, buffer cleared");
                                    break;
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    eprintln!("[{token:?}] Read error: {e}");
                                    closed = true;
                                    break;
                                }
                            }
                        }
                    }

                    if event.is_writable() && !to_return.is_empty() {
                        match client.ops.ok() {
                            Ok(()) => {
                                println!("[{token:?}] Echoed {} bytes", to_return.len());
                                to_return.clear();
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                eprintln!("[{token:?}] Write error: {e}");
                                closed = true;
                            }
                        }
                    }

                    if closed && let Some(client) = clients.remove(&token) {
                        poll.registry()
                            .deregister(&mut client.ops.unwrap_stream())?;
                        println!("[{token:?}] Disconnected");
                    }
                }
            }
        }
    }
}
