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

    pub fn write_info(&mut self, db_size: usize) -> std::io::Result<()> {
        let info = format!(
            "# Server\r\n\
             redis_version:7.0.0\r\n\
             redis_mode:standalone\r\n\
             os:Rust/mio\r\n\
             arch_bits:64\r\n\
             tcp_port:6379\r\n\
             uptime_in_seconds:42069\r\n\
             uptime_in_days:0\r\n\
             hz:10\r\n\
             executable:/usr/local/bin/reddis2\r\n\
             config_file:\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:1\r\n\
             blocked_clients:0\r\n\
             tracking_clients:0\r\n\
             maxclients:10000\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:1024000\r\n\
             used_memory_human:1000.00K\r\n\
             used_memory_rss:2048000\r\n\
             used_memory_rss_human:1.95M\r\n\
             used_memory_peak:2048000\r\n\
             used_memory_peak_human:1.95M\r\n\
             used_memory_lua:37888\r\n\
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
             # Keyspace\r\n\
             db0:keys={db_size},expires=0,avg_ttl=0\r\n"
        );
        self.write_bulk_string(info)
    }

    pub fn write_latency_histogram(&mut self) -> std::io::Result<()> {
        // Bogus LATENCY HISTOGRAM response in RESP2.
        // Top-level array: alternating command_name, details for 3 commands → 6 entries.
        // Each details array: "calls", <int>, "histogram_usec", <bucket array> → 4 entries.
        // Each bucket array: alternating boundary, cumulative_count.
        let mut buf = Vec::with_capacity(512);

        let commands: &[(&str, u64, &[(u64, u64)])] = &[
            ("get", 1000, &[(1, 50), (4, 200), (16, 600), (64, 900), (256, 1000)]),
            ("set", 800, &[(1, 40), (4, 160), (16, 480), (64, 720), (256, 800)]),
            ("ping", 5000, &[(1, 4500), (4, 4900), (16, 5000)]),
        ];

        // top-level array: commands.len() * 2
        buf.extend_from_slice(format!("*{}\r\n", commands.len() * 2).as_bytes());

        for (name, calls, buckets) in commands {
            // command name
            buf.extend_from_slice(format!("${}\r\n{}\r\n", name.len(), name).as_bytes());
            // details array: 4 entries (calls key, calls val, histogram_usec key, histogram_usec val)
            buf.extend_from_slice(b"*4\r\n");
            buf.extend_from_slice(b"$5\r\ncalls\r\n");
            buf.extend_from_slice(format!(":{}\r\n", calls).as_bytes());
            buf.extend_from_slice(b"$14\r\nhistogram_usec\r\n");
            buf.extend_from_slice(format!("*{}\r\n", buckets.len() * 2).as_bytes());
            for (bound, count) in *buckets {
                buf.extend_from_slice(format!(":{}\r\n:{}\r\n", bound, count).as_bytes());
            }
        }

        self.stream.write_all(&buf)
    }

    pub fn unwrap_stream(self) -> TcpStream {
        self.stream
    }
}
