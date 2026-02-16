use std::error::Error;
use std::fmt;
use std::io;
use std::num::ParseIntError;

#[derive(Debug)]
pub enum RedisError {
    IncompleteInput,
    Parse(String),
    IO(String),
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisError::Parse(message) => write!(f, "{message}"),
            RedisError::IncompleteInput => write!(f, "incomplete input"),
            RedisError::IO(message) => write!(f, "{message}"),
        }
    }
}

impl Error for RedisError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl From<io::Error> for RedisError {
    fn from(value: io::Error) -> Self {
        RedisError::IO(format!("IO error: {value}"))
    }
}

impl From<ParseIntError> for RedisError {
    fn from(value: ParseIntError) -> Self {
        RedisError::Parse(format!("Invalid input: {value}"))
    }
}
