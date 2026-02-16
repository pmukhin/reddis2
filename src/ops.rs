use mio::net::TcpStream;
use std::io::{Read, Write};

pub struct Ops {
    stream: TcpStream,
}

impl Ops {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }

    pub fn write_bulk_string<A: AsRef<[u8]>>(&mut self, bytes: A) -> std::io::Result<()> {
        self.stream
            .write_fmt(format_args!("${}\r\n", bytes.as_ref().len(),))?;
        self.stream.write_all(bytes.as_ref())?;
        self.stream.write_all("\r\n".as_bytes())?;
        Ok(())
    }

    pub fn write_array<A: AsRef<[u8]>>(&mut self, array: &[A]) -> std::io::Result<()> {
        self.stream
            .write_fmt(format_args!("*{}\r\n", array.len()))?;
        for elem in array {
            self.write_bulk_string(elem.as_ref())?;
        }
        Ok(())
    }

    pub fn ok(&mut self) -> std::io::Result<()> {
        self.stream.write_all("+OK\r\n".as_bytes())
    }

    pub fn pong(&mut self) -> std::io::Result<()> {
        self.stream.write_all("+PONG\r\n".as_bytes())
    }

    pub fn key_not_found(&mut self) -> std::io::Result<()> {
        self.stream.write_all("$-1\r\n".as_bytes())
    }

    pub fn wrong_type(&mut self, message: &str) -> std::io::Result<()> {
        self.stream
            .write_fmt(format_args!("-WRONGTYPE {}\r\n", message))
    }
    
    pub fn unwrap_stream(self) -> TcpStream {
        self.stream
    }
}
