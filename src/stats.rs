use compact_str::CompactString;
use histogram::Histogram;
use std::collections::HashMap;

pub struct CommandStats {}

impl CommandStats {
    pub fn make(latency_histograms: &HashMap<CompactString, Histogram>) -> String {
        let mut command_stats = String::from("# Commandstats\r\n");
        for (name, histogram) in latency_histograms {
            let mut calls: u64 = 0;
            let mut usec: u64 = 0;
            for bucket in histogram {
                calls += bucket.count();
                usec += bucket.end() * bucket.count();
            }
            let usec_per_call = if calls > 0 {
                usec as f64 / calls as f64
            } else {
                0.0
            };
            command_stats.push_str(&format!(
                "cmdstat_{name}:calls={calls},usec={usec},usec_per_call={usec_per_call:.2}\r\n"
            ));
        }
        command_stats
    }
}
