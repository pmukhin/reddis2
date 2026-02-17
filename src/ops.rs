use compact_str::CompactString;
use histogram::Histogram;
use mio::net::TcpStream;
use std::collections::HashMap;
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

    pub fn write_array<A: AsRef<[u8]>>(
        &mut self,
        array: impl Iterator<Item = A>,
        len: usize,
    ) -> std::io::Result<()> {
        self.stream.write_fmt(format_args!("*{}\r\n", len))?;
        for elem in array {
            self.write_bulk_string(elem.as_ref())?;
        }
        Ok(())
    }

    pub fn write_integer(&mut self, n: impl std::fmt::Display) -> std::io::Result<()> {
        self.stream.write_fmt(format_args!(":{}\r\n", n))
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

    pub fn wrong_type<A: AsRef<[u8]>>(&mut self, message: A) -> std::io::Result<()> {
        self.stream.write_all("-WRONGTYPE ".as_bytes())?;
        self.stream.write_all(message.as_ref())?;
        self.stream.write_all("\r\n".as_bytes())
    }

    pub(crate) fn generic_error<A: AsRef<[u8]>>(&mut self, message: A) -> std::io::Result<()> {
        self.stream.write_all("-ERR ".as_bytes())?;
        self.stream.write_all(message.as_ref())?;
        self.stream.write_all("\r\n".as_bytes())
    }

    pub fn write_latency_histogram(
        &mut self,
        histograms: &HashMap<CompactString, Histogram>,
        filter: &[&[u8]],
    ) -> std::io::Result<()> {
        // LATENCY HISTOGRAM response in RESP2.
        // Top-level array: alternating command_name, details.
        // Each details array: "calls", <int>, "histogram_usec", <bucket array>.
        // Each bucket array: alternating boundary, cumulative_count.
        let mut buf = Vec::with_capacity(512);

        let filtered: Vec<_> = if filter.is_empty() {
            histograms.iter().collect()
        } else {
            histograms
                .iter()
                .filter(|(name, _)| {
                    filter
                        .iter()
                        .any(|f| name.as_bytes().eq_ignore_ascii_case(f))
                })
                .collect()
        };

        buf.extend_from_slice(format!("*{}\r\n", filtered.len() * 2).as_bytes());

        for (name, histogram) in &filtered {
            let calls = histogram.iter().count();
            let buckets = histogram.iter().count();
            buf.extend_from_slice(format!("${}\r\n{}\r\n", name.len(), name).as_bytes());
            buf.extend_from_slice(b"*4\r\n");
            buf.extend_from_slice(b"$5\r\ncalls\r\n");
            buf.extend_from_slice(format!(":{}\r\n", calls).as_bytes());
            buf.extend_from_slice(b"$14\r\nhistogram_usec\r\n");
            buf.extend_from_slice(format!("*{}\r\n", buckets * 2).as_bytes());
            for bucket in *histogram {
                buf.extend_from_slice(
                    format!(":{}\r\n:{}\r\n", bucket.end(), bucket.count()).as_bytes(),
                );
            }
        }

        self.stream.write_all(&buf)
    }

    pub fn unwrap_stream(self) -> TcpStream {
        self.stream
    }
}
