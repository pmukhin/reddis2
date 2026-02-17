use crate::cmd::Command;
use crate::err::RedisError;

use crate::cmd::Info::{LibName, LibVersion};
use nom::{
    Err, IResult,
    bytes::complete::{tag, take_while},
    character::complete::digit0,
    combinator::opt,
    error::{ErrorKind, ParseError},
    multi::separated_list0,
};
use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt, num::ParseIntError};

#[derive(Debug)]
enum CmdCode {
    Ping,
    Set,
    Get,
    SetEx,
    Lpush,
    Rpush,
    LpushX,
    RpushX,
    Lpop,
    Rpop,
    Lrange,
    Hget,
    Hset,
    HMget,
    HMSet,
    Del,
    Incr,
    IncrBy,
    DbSize,
    Config,
    CommandDocs,
    FlushDb,
    ClientSetInfo,
    Ttl,
    LLen,
    HgetAll,
    HincrBy,
    Exists,
    Hexists,
    Hkeys,
    Sadd,
    Sismember,
    Sinter,
    Sunion,
    Sdiff,
    Scard,
    Smembers,
    Zadd,
    Zrange,
    Zrevrange,
    Zrank,
    Zrevrank,
    Zscore,
    Zrangebyscore,
    Zincrby,
    Zcard,
}

fn cmd(i: &[u8]) -> IResult<&[u8], CmdCode, ParseFailure> {
    let (i, cmd_str) = string(i)?;
    let v = match cmd_str {
        b"PING" => CmdCode::Ping,
        b"SETEX" => CmdCode::SetEx,
        b"SET" => CmdCode::Set,
        b"GET" => CmdCode::Get,
        b"LPUSHX" => CmdCode::LpushX,
        b"RPUSHX" => CmdCode::RpushX,
        b"LPUSH" => CmdCode::Lpush,
        b"RPUSH" => CmdCode::Rpush,
        b"LPOP" => CmdCode::Lpop,
        b"RPOP" => CmdCode::Rpop,
        b"LRANGE" => CmdCode::Lrange,
        b"HGET" => CmdCode::Hget,
        b"HSET" => CmdCode::Hset,
        b"HMGET" => CmdCode::HMget,
        b"HMSET" => CmdCode::HMSet,
        b"HGETALL" => CmdCode::HgetAll,
        b"HINCRBY" => CmdCode::HincrBy,
        b"EXISTS" => CmdCode::Exists,
        b"HEXISTS" => CmdCode::Hexists,
        b"HKEYS" => CmdCode::Hkeys,
        b"SADD" => CmdCode::Sadd,
        b"SISMEMBER" => CmdCode::Sismember,
        b"SINTER" => CmdCode::Sinter,
        b"SUNION" => CmdCode::Sunion,
        b"SDIFF" => CmdCode::Sdiff,
        b"SCARD" => CmdCode::Scard,
        b"SMEMBERS" => CmdCode::Smembers,
        b"ZADD" => CmdCode::Zadd,
        b"ZRANGE" => CmdCode::Zrange,
        b"ZREVRANGE" => CmdCode::Zrevrange,
        b"ZRANK" => CmdCode::Zrank,
        b"ZREVRANK" => CmdCode::Zrevrank,
        b"ZSCORE" => CmdCode::Zscore,
        b"ZRANGEBYSCORE" => CmdCode::Zrangebyscore,
        b"ZINCRBY" => CmdCode::Zincrby,
        b"ZCARD" => CmdCode::Zcard,
        b"DEL" => CmdCode::Del,
        b"INCRBY" => CmdCode::IncrBy,
        b"INCR" => CmdCode::Incr,
        b"DBSIZE" => CmdCode::DbSize,
        b"COMMAND" => CmdCode::CommandDocs,
        b"CONFIG" => CmdCode::Config,
        b"FLUSHDB" => CmdCode::FlushDb,
        b"CLIENT" => CmdCode::ClientSetInfo,
        b"TTL" => CmdCode::Ttl,
        b"LLEN" => CmdCode::LLen,
        unknown => {
            return Err(nom::Err::Error(ParseFailure(format!(
                "unknown command: {}",
                String::from_utf8_lossy(unknown)
            ))));
        }
    };
    Ok((i, v))
}

fn u_number<T>(i: &[u8]) -> IResult<&[u8], T, ParseFailure>
where
    T: FromStr<Err = ParseIntError>,
{
    let (i, v) = string(i)?;
    Ok((
        i,
        T::from_str(str::from_utf8(v).expect("invalid number")).expect("invalid number"),
    ))
}

fn value(i: &[u8]) -> IResult<&[u8], &[u8], ParseFailure> {
    let (i, _) = tag("$")(i)?;
    let (i, size_str) = digit0(i)?;
    let str_size =
        usize::from_str(str::from_utf8(size_str).expect("invalid number")).expect("invalid number");
    let (i, _) = tag("\r\n")(i)?;
    let value = &i[0..str_size];
    Ok((&i[str_size..], value))
}

fn string(i: &[u8]) -> IResult<&[u8], &[u8], ParseFailure> {
    let (i, value) = value(i)?;
    let (i, _) = tag("\r\n")(i)?;

    Ok((i, value))
}

fn push<'a, F>(i: &'a [u8], f: F) -> IResult<&'a [u8], Command<'a>, ParseFailure>
where
    F: Fn(&'a [u8], Vec<&'a [u8]>) -> Command<'a>,
{
    let (i, key) = string(i)?;
    let (i, raw_values) = separated_list0(tag("\r\n"), value)(i)?;

    Ok((i, f(key, raw_values)))
}

fn pop<'a, F>(i: &'a [u8], f: F) -> IResult<&'a [u8], Command<'a>, ParseFailure>
where
    F: Fn(&'a [u8], Option<usize>) -> Command<'a>,
{
    let (i, key) = string(i)?;
    let (i, count) = opt(u_number)(i)?;

    Ok((i, f(key, count)))
}

fn cmd_len(i: &[u8]) -> IResult<&[u8], usize, ParseFailure> {
    let (i, _) = tag([b'*'])(i)?;
    let (i, _u) = take_while(|c: u8| (48..=57).contains(&c))(i)?;
    let (i, _) = tag("\r\n")(i)?;
    Ok((
        i,
        usize::from_str(str::from_utf8(_u).expect("invalid number")).expect("invalid number"),
    ))
}

fn root(i: &[u8]) -> IResult<&[u8], Command<'_>, ParseFailure> {
    let (i, _) = opt(cmd_len)(i)?;
    let (i, cmd) = cmd(i)?;
    match cmd {
        CmdCode::Set => {
            let (i, key) = string(i)?;
            let (i, value) = string(i)?;
            let (i, maybe_set_opt) = opt(string)(i)?;
            match maybe_set_opt {
                None => Ok((i, Command::Set(key, value, None))),
                Some(opt) if opt.eq_ignore_ascii_case("EX".as_bytes()) => {
                    let (i, ttl) = u_number(i)?;
                    Ok((i, Command::Set(key, value, Some(Duration::from_secs(ttl)))))
                }
                Some(opt) if opt.eq_ignore_ascii_case("PX".as_bytes()) => {
                    let (i, ttl) = u_number(i)?;
                    Ok((
                        i,
                        Command::Set(key, value, Some(Duration::from_millis(ttl))),
                    ))
                }
                Some(opt) if opt.eq_ignore_ascii_case("NX".as_bytes()) => {
                    Ok((i, Command::SetNx(key, value)))
                }
                Some(opt) if opt.eq_ignore_ascii_case("XX".as_bytes()) => {
                    Ok((i, Command::SetXx(key, value)))
                }
                Some(opt) if opt.eq_ignore_ascii_case("GET".as_bytes()) => {
                    Ok((i, Command::SetAndGet(key, value)))
                }
                Some(opt) if opt.eq_ignore_ascii_case("KEEPTTL".as_bytes()) => {
                    Ok((i, Command::SetKeepTtl(key, value)))
                }
                Some(unknown) => Err(nom::Err::Error(ParseFailure(format!(
                    "unknown command: {}",
                    String::from_utf8_lossy(unknown)
                )))),
            }
        }
        CmdCode::Get => {
            let (i, key) = string(i)?;
            Ok((i, Command::Get(key)))
        }
        CmdCode::SetEx => {
            let (i, key) = string(i)?;
            let (i, ttl) = u_number(i)?;
            let (i, value) = string(i)?;
            let cmd = Command::Set(key, value, Some(Duration::from_secs(ttl)));
            Ok((i, cmd))
        }
        CmdCode::Lpush => push(i, Command::Lpush),
        CmdCode::Rpush => push(i, Command::Rpush),
        CmdCode::LpushX => push(i, Command::LpushX),
        CmdCode::RpushX => push(i, Command::RpushX),
        CmdCode::Lpop => pop(i, Command::Lpop),
        CmdCode::Rpop => pop(i, Command::Rpop),
        CmdCode::CommandDocs => Ok((i, Command::Docs)),
        CmdCode::Ping => Ok((i, Command::Ping)),
        CmdCode::Incr => {
            let (i, key) = string(i)?;
            Ok((i, Command::Incr(key)))
        }
        CmdCode::IncrBy => {
            let (i, key) = string(i)?;
            let (i, incr_by) = u_number::<i64>(i)?;
            Ok((i, Command::IncrBy(key, incr_by)))
        }
        CmdCode::Del => {
            let (i, raw_values) = separated_list0(tag("\r\n"), value)(i)?;
            let values = raw_values.to_vec();
            Ok((i, Command::Del(values)))
        }
        CmdCode::DbSize => Ok((i, Command::DbSize)),
        CmdCode::Hget => {
            let (i, key) = string(i)?;
            let (i, field) = string(i)?;
            Ok((i, Command::Hget(key, field)))
        }
        CmdCode::Hset => {
            let (i, key) = string(i)?;
            let (i, fields_and_values) = separated_list0(tag("\r\n"), value)(i)?;
            Ok((i, Command::HMset(key, fields_and_values)))
        }
        CmdCode::HMget => {
            let (i, key) = string(i)?;
            let (i, fields) = separated_list0(tag("\r\n"), value)(i)?;
            Ok((i, Command::HMget(key, fields)))
        }
        CmdCode::HMSet => {
            let (i, key) = string(i)?;
            let (i, fields_and_values) = separated_list0(tag("\r\n"), string)(i)?;
            Ok((i, Command::HMset(key, fields_and_values)))
        }
        CmdCode::HgetAll => {
            let (i, key) = string(i)?;
            Ok((i, Command::HgetAll(key)))
        }
        CmdCode::HincrBy => {
            let (i, key) = string(i)?;
            let (i, field) = string(i)?;
            let (i, incr_by) = u_number::<i64>(i)?;
            Ok((i, Command::HincrBy(key, field, incr_by)))
        }
        CmdCode::Exists => {
            let (i, key) = string(i)?;
            Ok((i, Command::Exists(key)))
        }
        CmdCode::Hexists => {
            let (i, key) = string(i)?;
            let (i, field) = string(i)?;
            Ok((i, Command::Hexists(key, field)))
        }
        CmdCode::Hkeys => {
            let (i, key) = string(i)?;
            Ok((i, Command::Hkeys(key)))
        }
        CmdCode::Sadd => push(i, Command::Sadd),
        CmdCode::Sismember => {
            let (i, key) = string(i)?;
            let (i, member) = string(i)?;
            Ok((i, Command::Sismember(key, member)))
        }
        CmdCode::Sinter => {
            let (i, keys) = separated_list0(tag("\r\n"), value)(i)?;
            Ok((i, Command::Sinter(keys.to_vec())))
        }
        CmdCode::Sunion => {
            let (i, keys) = separated_list0(tag("\r\n"), value)(i)?;
            Ok((i, Command::Sunion(keys.to_vec())))
        }
        CmdCode::Sdiff => {
            let (i, keys) = separated_list0(tag("\r\n"), value)(i)?;
            Ok((i, Command::Sdiff(keys.to_vec())))
        }
        CmdCode::Scard => {
            let (i, key) = string(i)?;
            Ok((i, Command::Scard(key)))
        }
        CmdCode::Smembers => {
            let (i, key) = string(i)?;
            Ok((i, Command::Smembers(key)))
        }
        CmdCode::Zadd => {
            let (i, key) = string(i)?;
            let (i, raw) = separated_list0(tag("\r\n"), value)(i)?;
            let mut members = Vec::new();
            for chunk in raw.chunks(2) {
                let score = i64::from_str(str::from_utf8(chunk[0]).expect("invalid score"))
                    .expect("invalid score");
                members.push((score, chunk[1]));
            }
            Ok((i, Command::Zadd(key, members)))
        }
        CmdCode::Zrange => {
            let (i, key) = string(i)?;
            let (i, start) = u_number::<isize>(i)?;
            let (i, stop) = u_number::<isize>(i)?;
            let mut withscores = false;
            let mut rev = false;
            let (mut i, flag1) = opt(string)(i)?;
            if let Some(f) = flag1 {
                if f.eq_ignore_ascii_case(b"WITHSCORES") {
                    withscores = true;
                }
                if f.eq_ignore_ascii_case(b"REV") {
                    rev = true;
                }
                let (i2, flag2) = opt(string)(i)?;
                i = i2;
                if let Some(f) = flag2 {
                    if f.eq_ignore_ascii_case(b"WITHSCORES") {
                        withscores = true;
                    }
                    if f.eq_ignore_ascii_case(b"REV") {
                        rev = true;
                    }
                }
            }
            if rev {
                Ok((i, Command::Zrevrange(key, start, stop, withscores)))
            } else {
                Ok((i, Command::Zrange(key, start, stop, withscores)))
            }
        }
        CmdCode::Zrevrange => {
            let (i, key) = string(i)?;
            let (i, start) = u_number::<isize>(i)?;
            let (i, stop) = u_number::<isize>(i)?;
            let (i, maybe_flag) = opt(string)(i)?;
            let withscores = matches!(maybe_flag, Some(f) if f.eq_ignore_ascii_case(b"WITHSCORES"));
            Ok((i, Command::Zrevrange(key, start, stop, withscores)))
        }
        CmdCode::Zrank => {
            let (i, key) = string(i)?;
            let (i, member) = string(i)?;
            Ok((i, Command::Zrank(key, member)))
        }
        CmdCode::Zrevrank => {
            let (i, key) = string(i)?;
            let (i, member) = string(i)?;
            Ok((i, Command::Zrevrank(key, member)))
        }
        CmdCode::Zscore => {
            let (i, key) = string(i)?;
            let (i, member) = string(i)?;
            Ok((i, Command::Zscore(key, member)))
        }
        CmdCode::Zrangebyscore => {
            let (i, key) = string(i)?;
            let (i, min) = u_number::<i64>(i)?;
            let (i, max) = u_number::<i64>(i)?;
            let (i, maybe_flag) = opt(string)(i)?;
            let withscores = matches!(maybe_flag, Some(f) if f.eq_ignore_ascii_case(b"WITHSCORES"));
            Ok((i, Command::Zrangebyscore(key, min, max, withscores)))
        }
        CmdCode::Zincrby => {
            let (i, key) = string(i)?;
            let (i, incr) = u_number::<i64>(i)?;
            let (i, member) = string(i)?;
            Ok((i, Command::Zincrby(key, incr, member)))
        }
        CmdCode::Config => Ok((i, Command::Config)),
        CmdCode::FlushDb => Ok((i, Command::FlushDb)),
        CmdCode::ClientSetInfo => {
            let (i, _) = string(i)?; // set info
            let (i, param) = string(i)?;
            let (i, value) = string(i)?;
            let param = if param.eq_ignore_ascii_case("LIB-NAME".as_bytes()) {
                LibName(value)
            } else {
                LibVersion(value)
            };
            Ok((i, Command::ClientSetInfo(param)))
        }
        CmdCode::Ttl => {
            let (i, key) = string(i)?;
            Ok((i, Command::Ttl(key)))
        }
        CmdCode::Lrange => {
            let (i, key) = string(i)?;
            let (i, start) = u_number(i)?;
            let (i, end) = u_number(i)?;
            Ok((i, Command::Lrange(key, start, end)))
        }
        CmdCode::LLen => {
            let (i, key) = string(i)?;
            Ok((i, Command::LLen(key)))
        }
        CmdCode::Zcard => {
            let (i, key) = string(i)?;
            Ok((i, Command::Zcard(key)))
        }
    }
}

#[derive(Debug)]
pub struct ParseFailure(String);

impl fmt::Display for ParseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parsing failure: `{:?}`", self.0)
    }
}

fn inline_to_resp(i: &[u8]) -> Vec<u8> {
    let line = i.strip_suffix(b"\r\n").unwrap_or(i);
    let parts: Vec<&[u8]> = line.split(|&b| b == b' ').collect();
    let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

pub fn parse(i: &[u8]) -> Result<Command<'_>, RedisError> {
    if !i.starts_with(b"*") && !i.starts_with(b"$") {
        let resp = inline_to_resp(i);
        let (_, cmd) = root(&resp)?;
        // SAFETY: inline commands (PING, DBSIZE, etc.) don't borrow from input
        let cmd: Command<'static> = unsafe { std::mem::transmute(cmd) };
        return Ok(cmd);
    }
    let (_, cmd) = root(i)?;
    Ok(cmd)
}

impl From<nom::Err<ParseFailure>> for RedisError {
    fn from(value: nom::Err<ParseFailure>) -> Self {
        match value {
            Err::Incomplete(_) => RedisError::IncompleteInput,
            Err::Error(ParseFailure(s)) => RedisError::Parse(format!("invalid input: {s}")),
            Err::Failure(_) => todo!(),
        }
    }
}

impl From<ParseIntError> for ParseFailure {
    fn from(value: ParseIntError) -> Self {
        ParseFailure(format!("can't parse int: {value}"))
    }
}

impl ParseError<&[u8]> for ParseFailure {
    fn from_error_kind(input: &[u8], kind: ErrorKind) -> Self {
        ParseFailure(format!("{:?}, {}", input, kind.description()))
    }

    fn append(input: &[u8], kind: ErrorKind, other: Self) -> Self {
        ParseFailure(format!(
            "{}, kind = {}, other = {}",
            String::from_utf8_lossy(input),
            kind.description(),
            other
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::Command;

    #[test]
    fn test_get() {
        let raw_cmd = "$3\r\nGET\r\n$3\r\naaa\r\n".as_bytes();
        assert_eq!(parse(raw_cmd).unwrap(), Command::Get("aaa".as_bytes()));
    }

    #[test]
    fn test_ping() {
        let raw_cmd = "$4\r\nPING\r\n".as_bytes();
        assert_eq!(parse(raw_cmd).unwrap(), Command::Ping);
    }

    #[test]
    fn test_set() {
        let raw_cmd = "$3\r\nSET\r\n$3\r\naaa\r\n$3\r\naaa\r\n".as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Set("aaa".as_bytes(), "aaa".as_bytes(), None)
        );
    }

    #[test]
    fn test_setex() {
        let raw_cmd = "$5\r\nSETEX\r\n$3\r\naaa\r\n$1\r\n5\r\n$3\r\naaa\r\n".as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Set(
                "aaa".as_bytes(),
                "aaa".as_bytes(),
                Some(Duration::from_secs(5))
            )
        );
    }

    #[test]
    fn test_lpush() {
        let raw_cmd =
            "$5\r\nLPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
                .as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Lpush(
                "aaa".as_bytes(),
                vec!["1", "2", "3", "4", "5"]
                    .iter()
                    .map(|v| v.as_bytes())
                    .collect(),
            )
        );
    }

    #[test]
    fn test_rpush() {
        let raw_cmd =
            "$5\r\nRPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
                .as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Rpush(
                "aaa".as_bytes(),
                vec!["1", "2", "3", "4", "5"]
                    .iter()
                    .map(|v| v.as_bytes())
                    .collect(),
            )
        );
    }

    #[test]
    fn test_lpushx() {
        let raw_cmd =
            "$6\r\nLPUSHX\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
                .as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::LpushX(
                "aaa".as_bytes(),
                vec!["1", "2", "3", "4", "5"]
                    .iter()
                    .map(|v| v.as_bytes())
                    .collect(),
            )
        );
    }

    #[test]
    fn test_rpushx() {
        let raw_cmd =
            "$6\r\nRPUSHX\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
                .as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::RpushX(
                "aaa".as_bytes(),
                vec!["1", "2", "3", "4", "5"]
                    .iter()
                    .map(|v| v.as_bytes())
                    .collect(),
            )
        );
    }

    #[test]
    fn test_lpop() {
        let raw_cmd = "$4\r\nLPOP\r\n$2\r\naa\r\n$1\r\n2\r\n".as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Lpop("aa".as_bytes(), Some(2))
        );
    }

    #[test]
    fn test_rpop() {
        let raw_cmd = "$4\r\nRPOP\r\n$2\r\naa\r\n$1\r\n2\r\n".as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Rpop("aa".as_bytes(), Some(2))
        );
    }

    #[test]
    fn test_del() {
        let raw_cmd = "$3\r\nDEL\r\n$3\r\naaa\r\n$3\r\nbbb\r\n$3\r\nccc\r\n".as_bytes();
        assert_eq!(
            parse(raw_cmd).unwrap(),
            Command::Del(vec!["aaa".as_bytes(), "bbb".as_bytes(), "ccc".as_bytes()]),
        );
    }

    #[test]
    fn test_conf() {
        let raw_cmd = "$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\nbbb\r\n".as_bytes();
        assert_eq!(parse(raw_cmd).unwrap(), Command::Config);
    }
}
