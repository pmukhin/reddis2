use tikv_jemalloc_ctl::{epoch, stats};

pub fn memory_usage() -> anyhow::Result<(usize, usize)> {
    let _ = epoch::advance().map_err(|e| anyhow::anyhow!("epoch::advance() failed: {e}"))?;
    let allocated = stats::allocated::mib()
        .map_err(|e| anyhow::anyhow!("stats::allocated::mib() failed: {e}"))?;
    let resident = stats::resident::mib()
        .map_err(|e| anyhow::anyhow!("stats::resident::mib() failed: {e}"))?;
    Ok((
        allocated
            .read()
            .map_err(|e| anyhow::anyhow!("stats::allocated::mib().read() failed: {e}"))?,
        resident
            .read()
            .map_err(|e| anyhow::anyhow!("stats::resident::mib().read() failed: {e}"))?,
    ))
}
