mod cmd;
mod err;
mod hmap_ops;
mod list_ops;
mod ops;
mod stored_value;

use crate::cmd::Command;
use crate::err::RedisError;
use hmap_ops::HMapOps;

use crate::list_ops::{HMapListOps, Popped};
use crate::stored_value::StoredValue;
use anyhow::Context;
use bytes::Bytes;
use itertools::Itertools;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
use tracing::info;

const SERVER: Token = Token(0);

struct Client {
    ops: ops::Ops,
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
                            let cmd = match client.ops.read(&mut buf) {
                                Ok(0) => {
                                    // Connection closed by peer
                                    closed = true;
                                    break;
                                }
                                Ok(n) => {
                                    total_read.extend_from_slice(&buf[..n]);
                                    let maybe_command = cmd::parser::parse(&total_read);
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
                                Command::Get(key) => match hmap.get(key) {
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
                                Command::Set(key, value, maybe_ttl) => {
                                    hmap.insert_alloc(
                                        key,
                                        value,
                                        maybe_ttl.map(|dur| Instant::now() + dur),
                                    );
                                    client.ops.ok()?;
                                }
                                Command::SetNx(key, value) => {
                                    hmap.set_if_not_exist(key, value);
                                    client.ops.ok()?;
                                }
                                Command::SetXx(key, value) => {
                                    hmap.update_if_exist(key, value);
                                    client.ops.ok()?;
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
                                }
                                Command::SetKeepTtl(key, value) => match hmap.get_mut(key) {
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
                                },
                                Command::Ping => {
                                    client.ops.pong()?;
                                }
                                Command::FlushDb => {
                                    client.ops.ok()?;
                                }
                                Command::Docs => {
                                    todo!("not implemented: Command::CommandDocs")
                                }
                                Command::DbSize => {
                                    todo!("not implemented: Command::DbSize")
                                }
                                Command::Config => {
                                    todo!("not implemented: Command::Config")
                                }
                                Command::Lpush(key, values) => match hmap.append(key, values) {
                                    Err(_) => client.ops.wrong_type("expected LIST")?,
                                    Ok(values_len) => {
                                        client.ops.write_bulk_string(values_len.to_string())?
                                    }
                                },
                                Command::Rpush(key, values) => match hmap.prepend(key, values) {
                                    Err(_) => client.ops.wrong_type("expected LIST")?,
                                    Ok(values_len) => {
                                        client.ops.write_bulk_string(values_len.to_string())?
                                    }
                                },
                                Command::LpushX(_, _) => {
                                    println!("Command::LpushX")
                                }
                                Command::RpushX(_, _) => {
                                    println!("Command::RpushX")
                                }
                                Command::Lpop(key, maybe_count) => {
                                    match hmap.pop_front(key, maybe_count) {
                                        Ok(Popped::None) => client.ops.key_not_found()?,
                                        Ok(Popped::Single(bytes)) => {
                                            client.ops.write_bulk_string(bytes)?
                                        }
                                        Ok(Popped::Multiple(values)) => {
                                            client.ops.write_array(values.as_slice())?
                                        }
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                    }
                                }
                                Command::Rpop(key, maybe_count) => {
                                    match hmap.pop_back(key, maybe_count) {
                                        Ok(Popped::None) => client.ops.key_not_found()?,
                                        Ok(Popped::Single(bytes)) => {
                                            client.ops.write_bulk_string(bytes)?
                                        }
                                        Ok(Popped::Multiple(values)) => {
                                            client.ops.write_array(values.as_slice())?
                                        }
                                        Err(e) => client.ops.wrong_type(e.to_string())?,
                                    }
                                }
                                Command::Del(keys) => {
                                    for key in keys {
                                        hmap.remove(key);
                                    }
                                    client.ops.ok()?;
                                }
                                Command::Incr(key) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::Plain(bytes)) => {
                                        let str = String::from_utf8_lossy(bytes);
                                        match str::parse::<i64>(&str) {
                                            Ok(num) => {
                                                let new_value = (num + 1).to_string();
                                                let value_as_bytes =
                                                    Bytes::copy_from_slice(new_value.as_bytes());
                                                client.ops.write_bulk_string(&value_as_bytes)?;
                                                hmap.insert(
                                                    Bytes::from(key.to_vec().into_boxed_slice()),
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
                                Command::IncrBy(key, incr_by) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::Plain(bytes)) => {
                                        let str = String::from_utf8_lossy(bytes);
                                        match str::parse::<i64>(&str) {
                                            Ok(num) => {
                                                let new_value = (num + incr_by).to_string();
                                                let value_as_bytes = Bytes::copy_from_slice(
                                                    new_value.into_bytes().as_slice(),
                                                );
                                                client.ops.write_bulk_string(&value_as_bytes)?;
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
                                Command::ClientSetInfo(_) => {
                                    client.ops.ok()?;
                                }
                                Command::Ttl(key) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::TtlPlain(_bytes, i)) => {
                                        let now = Instant::now();
                                        let diff = i.duration_since(now).as_secs();
                                        client.ops.write_bulk_string(diff.to_string())?;
                                    }
                                    _ => client.ops.wrong_type("stored value doesn't have TTL")?,
                                },
                                Command::Lrange(key, start, end) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::List(ll)) => {
                                        let real_start = (ll.len() as isize - start) as usize;
                                        let real_end = (ll.len() as isize - end) as usize;
                                        let vec = ll
                                            .iter()
                                            .skip(real_start)
                                            .take(real_end - real_start)
                                            .collect::<Vec<_>>();
                                        client.ops.write_array(vec.as_slice())?;
                                    }
                                    _ => client.ops.wrong_type("stored value isn't a list")?,
                                },
                                Command::LLen(key) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::List(ll)) => {
                                        client.ops.write_bulk_string(ll.len().to_string())?;
                                    }
                                    _ => client.ops.wrong_type("stored value isn't a list")?,
                                },
                                Command::Hget(key, field) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::Dict(dict)) => match dict.get(field) {
                                        None => client.ops.key_not_found()?,
                                        Some(value) => client.ops.write_bulk_string(value)?,
                                    },
                                    _ => client.ops.wrong_type("stored value isn't a dict")?,
                                },
                                Command::Hset(key, field, value) => match hmap.get_mut(key) {
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
                                    _ => client.ops.wrong_type("stored value isn't a dict")?,
                                },
                                Command::HMget(key, fields) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::Dict(dict)) => {
                                        let result = fields
                                            .iter()
                                            .filter_map(|f| dict.get(*f))
                                            .collect::<Vec<_>>();
                                        client.ops.write_array(result.as_slice())?;
                                    }
                                    _ => client.ops.wrong_type("stored value isn't a dict")?,
                                },
                                Command::HMset(key, fields_and_values) => {
                                    let stored_value = hmap
                                        .entry(Bytes::copy_from_slice(key))
                                        .or_insert(StoredValue::Dict(Default::default()));
                                    match stored_value {
                                        StoredValue::Dict(dict) => {
                                            fields_and_values
                                                .iter()
                                                .chunks(2)
                                                .into_iter()
                                                .for_each(|mut chunk| {
                                                    let field =
                                                        chunk.next().expect("HMSET is ill-formed");
                                                    let value =
                                                        chunk.next().expect("HMSET is ill-formed");
                                                    dict.insert(
                                                        Bytes::copy_from_slice(field),
                                                        Bytes::copy_from_slice(value),
                                                    );
                                                });
                                            client.ops.ok()?;
                                        }
                                        _ => client.ops.wrong_type("stored value isn't a dict")?,
                                    }
                                }
                                Command::HgetAll(key) => match hmap.get(key) {
                                    None => client.ops.key_not_found()?,
                                    Some(StoredValue::Dict(dict)) => {
                                        let fields_and_values = dict
                                            .iter()
                                            .flat_map(|(k, v)| [k, v])
                                            .collect::<Vec<_>>();
                                        client.ops.write_array(fields_and_values.as_slice())?;
                                    }
                                    _ => client.ops.wrong_type("stored value isn't a dict")?,
                                },
                            }
                            total_read.clear();
                            info!("[{token:?}] command is executed, buffer cleared");
                            break;
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
