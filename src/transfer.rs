use crate::{
    config::{Config, Route},
    transport,
};
use anyhow::{Context, Result, ensure};
use russh::ChannelMsg;
use russh_sftp::{client::SftpSession, protocol::{FileAttributes, OpenFlags}};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    io::SeekFrom,
    path::Path,
    fs,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt}, sync::Mutex};
use uuid::Uuid;

const MAX_ROUTE_ATTEMPTS: u32 = 3;
const ATOM_SIZE: usize = 256 * 1024;
const CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

fn default_verify() -> bool { true }

fn atom_size(config: &Config) -> usize {
    fn gcd(mut a: usize, mut b: usize) -> usize {
        while b != 0 { (a, b) = (b, a % b); }
        a
    }
    let mut size = gcd(ATOM_SIZE, config.settings.chunk_size);
    size = gcd(size, config.min_chunk_size);
    size = gcd(size, config.max_chunk_size);
    for &route_size in config.route_chunk_sizes.values() { size = gcd(size, route_size); }
    size
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Journal {
    #[serde(default)]
    pub version: u8,
    pub id: Uuid,
    pub direction: Direction,
    pub target: String,
    pub source: String,
    pub destination: String,
    pub temporary: String,
    pub size: u64,
    pub source_mtime: Option<u64>,
    pub sha256: String,
    #[serde(default = "default_verify")]
    pub verify: bool,
    pub chunk_size: usize,
    #[serde(default)]
    pub batch_size: usize,
    #[serde(default)]
    pub adaptive: bool,
    #[serde(default)]
    pub min_batch_size: usize,
    #[serde(default)]
    pub max_batch_size: usize,
    #[serde(default)]
    pub chunk_timeout_secs: Option<u64>,
    pub complete: Vec<bool>,
    pub published: bool,
    #[serde(default)]
    pub route_stats: BTreeMap<String, RouteTransferStats>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub state_dir: String,
    #[serde(default)]
    pub worker_host: String,
    #[serde(default)]
    pub worker_pid: u32,
    #[serde(default)]
    pub updated_at_unix_ms: u128,
    #[serde(skip, default = "Instant::now")]
    pub checkpoint_at: Instant,
    #[serde(skip)]
    pub uncheckpointed_bytes: u64,
}

fn worker_host() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|name| !name.is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        })
        .unwrap_or_default()
}

pub fn is_active(journal: &Journal) -> bool {
    if journal.published || journal.worker_pid == 0 {
        return false;
    }
    let same_host = !journal.worker_host.is_empty() && journal.worker_host == worker_host();
    #[cfg(unix)]
    if same_host {
        let Ok(pid) = i32::try_from(journal.worker_pid) else { return false };
        #[cfg(target_os = "linux")]
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            if stat.rsplit_once(") ").is_some_and(|(_, rest)| rest.starts_with('Z') || rest.starts_with('X')) {
                return false;
            }
        }
        let result = unsafe { libc::kill(pid, 0) };
        return result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
    }
    #[cfg(not(unix))]
    let _ = same_host;
    let lease_ms = u128::from(journal.chunk_timeout_secs.unwrap_or(120).max(120).saturating_add(60)) * 1000;
    now_unix_ms().saturating_sub(journal.updated_at_unix_ms) < lease_ms
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouteTransferStats {
    pub bytes: u64,
    pub last_bps: Option<f64>,
    pub last_at_unix_ms: u128,
    pub errors: u64,
    pub reconnects: u64,
    pub sessions: u64,
    #[serde(default)]
    pub connect_ms: u128,
    #[serde(default)]
    pub session_ms: u128,
    #[serde(default)]
    pub failed_session_ms: u128,
    #[serde(default)]
    pub chosen_batch_size: usize,
    #[serde(default)]
    pub fixed_batch_size: bool,
    #[serde(default)]
    pub observations: Vec<BatchObservation>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub cooldown_batches: u32,
    #[serde(default)]
    pub pending_connect_ms: u128,
    #[serde(default)]
    pub adaptive_max_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchObservation {
    pub size: usize,
    pub bytes: usize,
    pub elapsed_ms: u128,
}

fn median_rate(stats: &RouteTransferStats, size: usize) -> Option<f64> {
    let mut rates: Vec<f64> = stats.observations.iter().filter(|sample| sample.size == size)
        .map(|sample| sample.bytes as f64 * 1000.0 / sample.elapsed_ms.max(1) as f64).collect();
    if rates.len() < 3 { return None; }
    rates.sort_by(f64::total_cmp);
    Some(rates[rates.len() / 2])
}

fn observe_batch(stats: &mut RouteTransferStats, size: usize, bytes: usize, elapsed: Duration, min: usize, max: usize, timeout_secs: u64) {
    let elapsed_ms = elapsed.as_millis().saturating_add(std::mem::take(&mut stats.pending_connect_ms)).max(1);
    stats.observations.push(BatchObservation { size, bytes, elapsed_ms });
    let same_size = stats.observations.iter().filter(|sample| sample.size == size).count();
    if same_size > 5 {
        if let Some(index) = stats.observations.iter().position(|sample| sample.size == size) { stats.observations.remove(index); }
    }
    if bytes == 0 {
        stats.consecutive_failures += 1;
        if stats.consecutive_failures >= 2 {
            stats.chosen_batch_size = (size / 2).max(min);
            stats.cooldown_batches = 4;
        }
        return;
    }
    stats.consecutive_failures = 0;
    if stats.cooldown_batches > 0 {
        stats.cooldown_batches -= 1;
        return;
    }
    let Some(current) = median_rate(stats, size) else { return };
    let smaller = (size / 2).max(min);
    if smaller < size && median_rate(stats, smaller).is_some_and(|previous| current < previous * 0.9) {
        stats.chosen_batch_size = smaller;
        stats.adaptive_max_size = smaller;
        stats.cooldown_batches = 4;
    } else if size < max.min(if stats.adaptive_max_size == 0 { max } else { stats.adaptive_max_size }) {
        let candidate = size.saturating_mul(2).min(max).min(if stats.adaptive_max_size == 0 { max } else { stats.adaptive_max_size });
        if candidate as f64 / current <= timeout_secs as f64 * 0.8 {
            stats.chosen_batch_size = candidate;
        }
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

async fn note_session(shared: &Arc<Mutex<Journal>>, route: &str, connect: Duration) {
    let mut journal = shared.lock().await;
    journal.updated_at_unix_ms = now_unix_ms();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    if stats.sessions > 0 {
        stats.reconnects = stats.reconnects.saturating_add(1);
    }
    stats.sessions = stats.sessions.saturating_add(1);
    stats.connect_ms = stats.connect_ms.saturating_add(connect.as_millis());
    stats.pending_connect_ms = connect.as_millis();
    crate::debug_log!("transfer {}: route {} session opened", journal.id, route);
}

async fn note_worker_finished(
    shared: &Arc<Mutex<Journal>>,
    route: &str,
    elapsed: Duration,
    failed: bool,
    sftp: &SftpSession,
) -> Result<()> {
    let mut journal = shared.lock().await;
    journal.updated_at_unix_ms = now_unix_ms();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.session_ms = stats.session_ms.saturating_add(elapsed.as_millis());
    if failed {
        stats.failed_session_ms = stats.failed_session_ms.saturating_add(elapsed.as_millis());
    }
    crate::debug_log!("transfer {}: route {} session ended after {} ms, failed={failed}", journal.id, route, elapsed.as_millis());
    save(sftp, &journal).await?;
    journal.uncheckpointed_bytes = 0;
    journal.checkpoint_at = Instant::now();
    Ok(())
}

async fn note_error(shared: &Arc<Mutex<Journal>>, route: &str, error: &anyhow::Error, sftp: &SftpSession) -> Result<()> {
    let mut journal = shared.lock().await;
    journal.updated_at_unix_ms = now_unix_ms();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.errors = stats.errors.saturating_add(1);
    journal.last_error = Some(format!("{error:#}"));
    crate::debug_log!("transfer {}: route {} error: {error:#}", journal.id, route);
    save(sftp, &journal).await
}

async fn note_failed_batch(shared: &Arc<Mutex<Journal>>, route: &str, selected_size: usize, started: Instant) {
    let mut journal = shared.lock().await;
    if journal.adaptive {
        let min = journal.min_batch_size.max(journal.chunk_size);
        let max = journal.max_batch_size.max(min);
        let timeout_secs = journal.chunk_timeout_secs.unwrap_or(120);
        let stats = journal.route_stats.entry(route.to_owned()).or_default();
        if !stats.fixed_batch_size {
            observe_batch(stats, selected_size, 0, started.elapsed(), min, max, timeout_secs);
            crate::debug_log!("transfer {}: route {} batch failed; next size {} bytes", journal.id, route, route_batch_size(&journal, route));
        }
    }
}

async fn note_failed_connection(shared: &Arc<Mutex<Journal>>, route: &str, started: Instant) {
    let mut journal = shared.lock().await;
    if journal.adaptive {
        let selected_size = route_batch_size(&journal, route);
        let min = journal.min_batch_size.max(journal.chunk_size);
        let max = journal.max_batch_size.max(min);
        let timeout_secs = journal.chunk_timeout_secs.unwrap_or(120);
        let stats = journal.route_stats.entry(route.to_owned()).or_default();
        if !stats.fixed_batch_size {
            observe_batch(stats, selected_size, 0, started.elapsed(), min, max, timeout_secs);
            crate::debug_log!("transfer {}: route {} connection failed after {} ms; next batch size {} bytes",
                journal.id, route, started.elapsed().as_millis(), route_batch_size(&journal, route));
        }
    }
}

fn retry_route(error: &anyhow::Error, attempts: u32) -> bool {
    !transport::is_permanent(error) && attempts < MAX_ROUTE_ATTEMPTS
}

async fn note_batch(
    shared: &Arc<Mutex<Journal>>,
    sftp: &SftpSession,
    route: &str,
    indices: &[usize],
    bytes: usize,
    selected_size: usize,
    started: Instant,
) -> Result<()> {
    let elapsed = started.elapsed();
    let mut journal = shared.lock().await;
    for &index in indices {
        journal.complete[index] = true;
    }
    journal.updated_at_unix_ms = now_unix_ms();
    journal.last_error = None;
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.bytes = stats.bytes.saturating_add(bytes as u64);
    let sample_bps = bytes as f64 / elapsed.as_secs_f64().max(0.001);
    stats.last_bps = Some(
        stats
            .last_bps
            .map_or(sample_bps, |old| old * 0.7 + sample_bps * 0.3),
    );
    stats.last_at_unix_ms = now_unix_ms();
    crate::debug_log!("transfer {}: atoms {}..={} offset={} bytes={} route={} elapsed={} ms committed", journal.id, indices[0], indices[indices.len() - 1], indices[0] as u64 * journal.chunk_size as u64, bytes, route, elapsed.as_millis());
    journal.uncheckpointed_bytes = journal.uncheckpointed_bytes.saturating_add(bytes as u64);
    if journal.uncheckpointed_bytes >= CHECKPOINT_BYTES || journal.checkpoint_at.elapsed() >= CHECKPOINT_INTERVAL {
        save(sftp, &journal).await?;
        journal.uncheckpointed_bytes = 0;
        journal.checkpoint_at = Instant::now();
    }
    if journal.adaptive && bytes >= selected_size {
        let min = journal.min_batch_size.max(journal.chunk_size);
        let max = journal.max_batch_size.max(min);
        let timeout_secs = journal.chunk_timeout_secs.unwrap_or(120);
        let stats = journal.route_stats.entry(route.to_owned()).or_default();
        if !stats.fixed_batch_size {
            observe_batch(stats, selected_size, bytes, started.elapsed(), min, max, timeout_secs);
            crate::debug_log!("transfer {}: route {} next batch size {} bytes", journal.id, route, route_batch_size(&journal, route));
        }
    }
    crate::debug::write_stderr(format_args!(
        "{}: {}/{} chunks via {}",
        journal.id,
        journal.complete.iter().filter(|&&done| done).count(),
        journal.complete.len(),
        route
    ));
    Ok(())
}

fn claim_group(queue: &mut VecDeque<usize>, max_atoms: usize) -> Vec<usize> {
    let Some(first) = queue.pop_front() else { return Vec::new() };
    let mut group = vec![first];
    while group.len() < max_atoms.max(1)
        && queue.front().is_some_and(|&next| next == group[group.len() - 1] + 1)
    {
        group.push(queue.pop_front().expect("front was checked"));
    }
    group
}

fn route_batch_size(journal: &Journal, route: &str) -> usize {
    if journal.version < 2 { return journal.chunk_size; }
    journal.route_stats.get(route).and_then(|stats| (stats.chosen_batch_size > 0).then_some(stats.chosen_batch_size))
        .unwrap_or(journal.batch_size).max(journal.chunk_size)
}

fn initial_route_stats(config: &Config) -> BTreeMap<String, RouteTransferStats> {
    config.route_chunk_sizes.iter().map(|(route, &size)| {
        (route.clone(), RouteTransferStats { chosen_batch_size: size, fixed_batch_size: true, ..Default::default() })
    }).collect()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Put,
    Get,
}

async fn state_dir(sftp: &SftpSession) -> Result<String> {
    let home = sftp.canonicalize(".").await?;
    let root = format!("{home}/.mssh");
    let dir = format!("{root}/transfers");
    if !sftp.try_exists(&root).await? {
        sftp.create_dir(&root).await?;
    }
    sftp.set_metadata(&root, FileAttributes { permissions: Some(0o700), ..Default::default() }).await?;
    if !sftp.try_exists(&dir).await? {
        sftp.create_dir(&dir).await?;
    }
    sftp.set_metadata(&dir, FileAttributes { permissions: Some(0o700), ..Default::default() }).await?;
    crate::debug_log!("transfer state: remote directory {dir}");
    Ok(dir)
}

async fn save(sftp: &SftpSession, journal: &Journal) -> Result<()> {
    let path = format!("{}/{}.part.json", journal.state_dir, journal.id);
    let backup = format!("{path}.bak");
    let temporary = format!("{path}.new-{}", Uuid::new_v4());
    let bytes = serde_json::to_vec(journal)?;
    let mut file = sftp.open_with_flags_and_attributes(
        &temporary,
        OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
        FileAttributes { permissions: Some(0o600), ..Default::default() },
    ).await?;
    file.write_all(&bytes).await?;
    file.close().await?;
    crate::debug_log!("transfer {}: wrote {} journal bytes to remote temporary file", journal.id, bytes.len());
    if sftp.try_exists(&backup).await? {
        if sftp.try_exists(&path).await? { sftp.remove_file(&backup).await?; }
        else { sftp.rename(&backup, &path).await?; }
    }
    let had_previous = sftp.try_exists(&path).await?;
    if had_previous { sftp.rename(&path, &backup).await?; }
    if let Err(error) = sftp.rename(&temporary, &path).await {
        if had_previous { let _ = sftp.rename(&backup, &path).await; }
        return Err(error.into());
    }
    if had_previous { let _ = sftp.remove_file(&backup).await; }
    crate::debug_log!("transfer {}: remote journal committed, {}/{} chunks complete", journal.id, journal.complete.iter().filter(|&&done| done).count(), journal.complete.len());
    Ok(())
}

async fn remove_stale_journal_writes(sftp: &SftpSession, journal: &Journal) -> Result<()> {
    let prefix = format!("{}.part.json.new-", journal.id);
    for entry in sftp.read_dir(&journal.state_dir).await? {
        let name = entry.file_name();
        if name.starts_with(&prefix) {
            crate::debug_log!("transfer {}: removing abandoned journal write {name}", journal.id);
            sftp.remove_file(format!("{}/{name}", journal.state_dir)).await?;
        }
    }
    Ok(())
}

pub async fn list(config: &Config, target: &str) -> Result<Vec<Journal>> {
    crate::debug_log!("transfer state: listing journals for {target}");
    let connection = connect_any(config, target).await?;
    let sftp = sftp(&connection).await?;
    let dir = state_dir(&sftp).await?;
    let mut journals = BTreeMap::<Uuid, Journal>::new();
    for entry in sftp.read_dir(&dir).await? {
        let name = entry.file_name();
        if !name.ends_with(".part.json") && !name.ends_with(".part.json.bak") {
            continue;
        }
        let bytes = sftp.read(format!("{dir}/{name}")).await?;
        if let Ok(journal) = serde_json::from_slice::<Journal>(&bytes) {
            if journal.target == target && (!journals.contains_key(&journal.id) || name.ends_with(".part.json")) {
                journals.insert(journal.id, journal);
            }
        }
    }
    crate::debug_log!("transfer state: found {} journals for {target}", journals.len());
    Ok(journals.into_values().collect())
}

pub async fn discard(config: &Config, journal: &Journal) -> Result<()> {
    crate::debug_log!("transfer {}: discarding partial data and remote journal", journal.id);
    ensure!(!is_active(journal), "transfer is still active");
    let connection = connect_any(config, &journal.target).await?;
    let sftp = sftp(&connection).await?;
    if matches!(journal.direction, Direction::Put) && sftp.try_exists(&journal.temporary).await? {
        sftp.remove_file(&journal.temporary).await?;
    }
    if matches!(journal.direction, Direction::Get) && Path::new(&journal.temporary).exists() {
        std::fs::remove_file(&journal.temporary)?;
    }
    let path = format!("{}/{}.part.json", journal.state_dir, journal.id);
    if sftp.try_exists(&path).await? { sftp.remove_file(&path).await?; }
    let backup = format!("{path}.bak");
    if sftp.try_exists(&backup).await? { sftp.remove_file(&backup).await?; }
    remove_stale_journal_writes(&sftp, journal).await?;
    Ok(())
}

pub(crate) async fn sftp(connection: &transport::Connection) -> Result<SftpSession> {
    crate::debug_log!("SFTP: opening subsystem channel");
    let channel = connection.final_handle().channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    Ok(SftpSession::new_with_config(channel.into_stream(), russh_sftp::client::Config {
        request_timeout_secs: 300,
        ..Default::default()
    }).await?)
}

pub(crate) async fn connect_any(config: &Config, target: &str) -> Result<transport::Connection> {
    let mut last_error = None;
    for route in config.routes_for(target) {
        crate::debug_log!("SFTP: trying route {} to {target}", route.name);
        match transport::connect(config, &route).await {
            Ok(connection) => { crate::debug_log!("SFTP: route {} connected", route.name); return Ok(connection) },
            Err(error) => { crate::debug_log!("SFTP: route {} failed: {error:#}", route.name); last_error = Some(error) },
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no route to target {target}")))
}

fn shell_quote(path: &str) -> String {
    format!("'{}'", path.replace('\'', "'\\''"))
}

async fn remote_hash_command(connection: &transport::Connection, path: &str) -> Result<String> {
    let work = async {
        let mut channel = connection.final_handle().channel_open_session().await?;
        channel.exec(true, format!("sha256sum -- {}", shell_quote(path))).await?;
        let mut output = Vec::new();
        let mut exit_status = None;
        while let Some(message) = channel.wait().await {
            match message {
                ChannelMsg::Data { data } => {
                    ensure!(output.len() + data.len() <= 8192, "remote SHA-256 output is too long");
                    output.extend_from_slice(&data);
                }
                ChannelMsg::ExitStatus { exit_status: code } => exit_status = Some(code),
                ChannelMsg::Close => break,
                _ => {}
            }
        }
        ensure!(exit_status == Some(0), "remote SHA-256 command failed");
        let output = std::str::from_utf8(&output)?;
        let digest = output.split_whitespace().next().context("remote SHA-256 output is empty")?;
        ensure!(digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "remote SHA-256 output is invalid");
        Ok::<String, anyhow::Error>(digest.to_ascii_lowercase())
    };
    tokio::time::timeout(Duration::from_secs(3600), work).await.context("remote SHA-256 command timed out")?
}

async fn remote_hash_sftp(sftp: &SftpSession, path: &str, buffer_size: usize) -> Result<String> {
    let mut remote = sftp.open(path).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; buffer_size.min(1024 * 1024)];
    loop {
        let n = remote.read(&mut buffer).await?;
        if n == 0 { break; }
        hash.update(&buffer[..n]);
    }
    remote.close().await?;
    Ok(hex::encode(hash.finalize()))
}

async fn remote_hash(connection: &transport::Connection, sftp: &SftpSession, path: &str, buffer_size: usize) -> Result<String> {
    match remote_hash_command(connection, path).await {
        Ok(digest) => {
            crate::debug_log!("hash: computed remote SHA-256 on server for {path}");
            Ok(digest)
        }
        Err(error) => {
            crate::debug_log!("hash: remote SHA-256 command unavailable ({error:#}); reading {path} over SFTP");
            remote_hash_sftp(sftp, path, buffer_size).await
        }
    }
}

async fn local_hash(path: &Path) -> Result<(u64, String, Option<u64>)> {
    crate::debug_log!("hash: reading local file {}", path.display());
    let mut file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    ensure!(metadata.is_file(), "source is not a regular file");
    let modified = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|t| t.as_secs());
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        total += n as u64;
    }
    let after = file.metadata().await?;
    ensure!(
        total == metadata.len()
            && after.len() == metadata.len()
            && after.modified()? == metadata.modified()?,
        "source changed while hashing"
    );
    let digest = hex::encode(hash.finalize());
    crate::debug_log!("hash: {} bytes read from {}, SHA-256 {}", metadata.len(), path.display(), digest);
    Ok((metadata.len(), digest, modified))
}

async fn local_source(path: &Path, verify: bool) -> Result<(u64, String, Option<u64>)> {
    if verify { return local_hash(path).await; }
    let metadata = tokio::fs::metadata(path).await?;
    ensure!(metadata.is_file(), "source is not a regular file");
    let modified = metadata.modified().ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|time| time.as_secs());
    Ok((metadata.len(), String::new(), modified))
}

pub async fn put(config: &Config, target: &str, source: &Path, destination: &str) -> Result<Uuid> {
    crate::debug_log!("upload: {} -> {}:{}", source.display(), target, destination);
    let routes = config.routes_for(target);
    ensure!(!routes.is_empty(), "no route to target {target}");
    ensure!(
        destination.starts_with('/'),
        "remote destination must be an absolute path"
    );
    let (size, sha256, source_mtime) = local_source(source, config.verify).await?;
    let id = Uuid::new_v4();
    let temporary = format!("{destination}.mssh-{id}.part");
    let atom_size = atom_size(config);
    let initial_batch_size = if config.auto_chunk {
        config.settings.chunk_size.clamp(config.min_chunk_size, config.max_chunk_size)
    } else { config.settings.chunk_size };
    let chunks = size.div_ceil(atom_size as u64) as usize;
    crate::debug_log!("upload {id}: source size={size} bytes, chunks={chunks}, SHA-256 {sha256}");
    let connection = connect_any(config, target).await?;
    let sftp = sftp(&connection).await?;
    let mut journal = Journal {
        version: 2,
        id,
        direction: Direction::Put,
        target: target.into(),
        source: source.to_string_lossy().into_owned(),
        destination: destination.into(),
        temporary,
        size,
        source_mtime,
        sha256,
        verify: config.verify,
        chunk_size: atom_size,
        batch_size: initial_batch_size,
        adaptive: config.auto_chunk,
        min_batch_size: config.min_chunk_size,
        max_batch_size: config.max_chunk_size,
        chunk_timeout_secs: config.chunk_timeout_secs,
        complete: vec![false; chunks],
        published: false,
        route_stats: initial_route_stats(config),
        last_error: None,
        state_dir: state_dir(&sftp).await?,
        worker_host: worker_host(),
        worker_pid: std::process::id(),
        updated_at_unix_ms: now_unix_ms(),
        checkpoint_at: Instant::now(),
        uncheckpointed_bytes: 0,
    };
    ensure!(
        !sftp.try_exists(destination).await?,
        "destination already exists"
    );
    let file = sftp
        .open_with_flags(
            &journal.temporary,
            OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
        )
        .await?;
    file.close().await?;
    crate::debug_log!("upload {id}: created remote part {}", journal.temporary);
    if let Err(error) = save(&sftp, &journal).await {
        let _ = sftp.remove_file(&journal.temporary).await;
        return Err(error);
    }
    process_put(config, &mut journal, &connection, &sftp).await?;
    Ok(id)
}

async fn process_put(
    config: &Config,
    journal: &mut Journal,
    control_connection: &transport::Connection,
    control_sftp: &SftpSession,
) -> Result<()> {
    crate::debug_log!("upload {}: processing {} pending chunks", journal.id, journal.complete.iter().filter(|&&done| !done).count());
    let source = Path::new(&journal.source);
    let (size, sha256, mtime) = local_source(source, journal.verify).await?;
    ensure!(
        size == journal.size && sha256 == journal.sha256 && mtime == journal.source_mtime,
        "source changed since transfer began"
    );
    let mut routes: Vec<Route> = config
        .routes_for(&journal.target)
        .into_iter()
        .take(config.settings.max_parallel_routes)
        .collect();
    ensure!(!routes.is_empty(), "no route to target {}", journal.target);
    crate::debug_log!("upload {}: using routes {}", journal.id, routes.iter().map(|route| route.name.as_str()).collect::<Vec<_>>().join(", "));
    let shared = Arc::new(Mutex::new(journal.clone()));
    let config = Arc::new(config.clone());
    let mut failures = BTreeMap::<String, u32>::new();
    loop {
        let pending: Vec<usize> = shared
            .lock()
            .await
            .complete
            .iter()
            .enumerate()
            .filter_map(|(i, done)| (!done).then_some(i))
            .collect();
        if pending.is_empty() {
            break;
        }
        crate::debug_log!("upload {}: scheduling {} chunks", journal.id, pending.len());
        ensure!(
            !routes.is_empty(),
            "all routes failed; transfer can be resumed later"
        );
        let queue = Arc::new(Mutex::new(VecDeque::from(pending)));
        let mut workers = tokio::task::JoinSet::new();
        for route in routes.iter().cloned() {
            let config = config.clone();
            let shared = shared.clone();
            let queue = queue.clone();
            workers.spawn(async move {
                let name = route.name.clone();
                let started = Instant::now();
                let result = put_worker(config, route, shared, queue).await;
                (name, started.elapsed(), result)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = workers.join_next().await {
            let (name, elapsed, outcome) = result?;
            note_worker_finished(&shared, &name, elapsed, outcome.is_err(), &control_sftp).await?;
            if let Err(error) = outcome {
                crate::debug::write_stderr(format_args!("route {name}: {error:#}"));
                note_error(&shared, &name, &error, &control_sftp).await?;
                let attempts = failures.entry(name.clone()).or_default();
                *attempts += 1;
                if !retry_route(&error, *attempts) {
                    failed.push(name);
                }
            }
        }
        routes.retain(|r| !failed.contains(&r.name));
        if !routes.is_empty() && shared.lock().await.complete.iter().any(|&done| !done) {
            let attempts = failures.values().copied().max().unwrap_or(0);
            tokio::time::sleep(Duration::from_secs(u64::from(attempts.min(2)))).await;
        }
    }
    *journal = shared.lock().await.clone();
    let (source_size, _, source_mtime) = local_source(Path::new(&journal.source), false).await?;
    ensure!(source_size == journal.size && source_mtime == journal.source_mtime,
        "source changed during upload; temporary file retained");
    ensure!(control_sftp.metadata(&journal.temporary).await?.len() == journal.size,
        "remote part size differs; temporary file retained");
    if journal.verify {
        let digest = remote_hash(control_connection, control_sftp, &journal.temporary, journal.chunk_size).await?;
        ensure!(digest == journal.sha256, "remote SHA-256 differs; temporary file retained");
        crate::debug_log!("upload {}: remote SHA-256 verified", journal.id);
    }
    ensure!(
        !control_sftp.try_exists(&journal.destination).await?,
        "destination appeared during upload"
    );
    let linked = control_sftp
        .hardlink(&journal.temporary, &journal.destination)
        .await?;
    ensure!(
        linked,
        "server lacks hardlink@openssh.com; verified temporary file retained at {}",
        journal.temporary
    );
    journal.published = true;
    crate::debug_log!("upload {}: published {}", journal.id, journal.destination);
    save(control_sftp, journal).await?;
    // The published name is safe now; cleanup failure is nonfatal.
    let _ = control_sftp.remove_file(&journal.temporary).await;
    Ok(())
}

async fn put_worker(
    config: Arc<Config>,
    route: Route,
    shared: Arc<Mutex<Journal>>,
    queue: Arc<Mutex<VecDeque<usize>>>,
) -> Result<()> {
    let connect_started = Instant::now();
    let connection = match transport::connect(&config, &route).await {
        Ok(connection) => connection,
        Err(error) => { note_failed_connection(&shared, &route.name, connect_started).await; return Err(error); }
    };
    let sftp = match sftp(&connection).await {
        Ok(sftp) => sftp,
        Err(error) => { note_failed_connection(&shared, &route.name, connect_started).await; return Err(error); }
    };
    note_session(&shared, &route.name, connect_started.elapsed()).await;
    let snapshot = shared.lock().await.clone();
    let mut local = tokio::fs::File::open(&snapshot.source).await?;
    loop {
        let selected_size = { let state = shared.lock().await; route_batch_size(&state, &route.name) };
        let max_atoms = (selected_size / snapshot.chunk_size).max(1);
        let group = claim_group(&mut *queue.lock().await, max_atoms);
        if group.is_empty() {
            break;
        }
        let chunk_started = Instant::now();
        let offset = group[0] as u64 * snapshot.chunk_size as u64;
        let length = (snapshot.size - offset).min(group.len() as u64 * snapshot.chunk_size as u64) as usize;
        let batch_io = async {
            let mut buffer = vec![0u8; length];
            local.seek(SeekFrom::Start(offset)).await?;
            local.read_exact(&mut buffer).await?;
            let mut remote = sftp.open_with_flags(&snapshot.temporary, OpenFlags::WRITE).await?;
            remote.seek(SeekFrom::Start(offset)).await?;
            remote.write_all(&buffer).await?;
            remote.close().await?;
            Ok::<(), anyhow::Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(snapshot.chunk_timeout_secs.unwrap_or(120)), batch_io).await;
        match result {
            Ok(Ok(())) => {},
            Ok(Err(error)) => { note_failed_batch(&shared, &route.name, selected_size, chunk_started).await; return Err(error); },
            Err(_) => {
                note_failed_batch(&shared, &route.name, selected_size, chunk_started).await;
                anyhow::bail!("batch starting at atom {} exceeded {} second timeout", group[0], snapshot.chunk_timeout_secs.unwrap_or(120));
            }
        }
        note_batch(&shared, &sftp, &route.name, &group, length, selected_size, chunk_started).await?;
    }
    Ok(())
}

pub async fn continue_transfer(config: &Config, journal: &Journal) -> Result<()> {
    crate::debug_log!("transfer {}: continuing {:?} to {}", journal.id, journal.direction, journal.destination);
    let mut journal = journal.clone();
    ensure!(!journal.published, "transfer already published");
    ensure!(!is_active(&journal), "transfer is still active");
    journal.worker_host = worker_host();
    journal.worker_pid = std::process::id();
    journal.updated_at_unix_ms = now_unix_ms();
    // Completed chunks have been written (and uploads read back) before being
    // recorded. Keep them on resume; the full-file digest checks the result.
    let connection = connect_any(config, &journal.target).await?;
    let sftp = sftp(&connection).await?;
    remove_stale_journal_writes(&sftp, &journal).await?;
    save(&sftp, &journal).await?;
    match journal.direction {
        Direction::Put => process_put(config, &mut journal, &connection, &sftp).await,
        Direction::Get => process_get(config, &mut journal, &sftp).await,
    }
}

pub async fn get(config: &Config, target: &str, source: &str, destination: &Path) -> Result<Uuid> {
    crate::debug_log!("download: {}:{} -> {}", target, source, destination.display());
    let routes = config.routes_for(target);
    ensure!(!routes.is_empty(), "no route to target {target}");
    ensure!(
        source.starts_with('/'),
        "remote source must be an absolute path"
    );
    ensure!(!destination.exists(), "local destination already exists");
    let connection = connect_any(config, target).await?;
    let sftp = sftp(&connection).await?;
    let metadata = sftp.metadata(source).await?;
    ensure!(
        metadata.file_type().is_file(),
        "remote source is not a regular file"
    );
    let size = metadata.len();
    let source_mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|t| t.as_secs());
    let sha256 = if config.verify { remote_hash(&connection, &sftp, source, config.settings.chunk_size).await? }
        else { String::new() };
    crate::debug_log!("download: source size={size} bytes, SHA-256 {sha256}");
    let id = Uuid::new_v4();
    let atom_size = atom_size(config);
    let initial_batch_size = if config.auto_chunk {
        config.settings.chunk_size.clamp(config.min_chunk_size, config.max_chunk_size)
    } else { config.settings.chunk_size };
    let temporary = format!("{}.mssh-{id}.part", destination.display());
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(&temporary)?;
    crate::debug_log!("download {id}: created local part {temporary}");
    let mut journal = Journal {
        version: 2,
        id,
        direction: Direction::Get,
        target: target.into(),
        source: source.into(),
        destination: destination.to_string_lossy().into_owned(),
        temporary,
        size,
        source_mtime,
        sha256,
        verify: config.verify,
        chunk_size: atom_size,
        batch_size: initial_batch_size,
        adaptive: config.auto_chunk,
        min_batch_size: config.min_chunk_size,
        max_batch_size: config.max_chunk_size,
        chunk_timeout_secs: config.chunk_timeout_secs,
        complete: vec![false; size.div_ceil(atom_size as u64) as usize],
        published: false,
        route_stats: initial_route_stats(config),
        last_error: None,
        state_dir: state_dir(&sftp).await?,
        worker_host: worker_host(),
        worker_pid: std::process::id(),
        updated_at_unix_ms: now_unix_ms(),
        checkpoint_at: Instant::now(),
        uncheckpointed_bytes: 0,
    };
    if let Err(error) = save(&sftp, &journal).await {
        let _ = fs::remove_file(&journal.temporary);
        return Err(error);
    }
    process_get(config, &mut journal, &sftp).await?;
    Ok(id)
}

async fn process_get(config: &Config, journal: &mut Journal, control_sftp: &SftpSession) -> Result<()> {
    crate::debug_log!("download {}: processing {} pending chunks", journal.id, journal.complete.iter().filter(|&&done| !done).count());
    let mut routes: Vec<Route> = config
        .routes_for(&journal.target)
        .into_iter()
        .take(config.settings.max_parallel_routes)
        .collect();
    ensure!(!routes.is_empty(), "no route to target {}", journal.target);
    crate::debug_log!("download {}: using routes {}", journal.id, routes.iter().map(|route| route.name.as_str()).collect::<Vec<_>>().join(", "));
    ensure!(
        !Path::new(&journal.destination).exists(),
        "local destination already exists"
    );
    ensure!(
        Path::new(&journal.temporary).exists(),
        "local temporary file is missing"
    );
    let shared = Arc::new(Mutex::new(journal.clone()));
    let config = Arc::new(config.clone());
    let mut failures = BTreeMap::<String, u32>::new();
    loop {
        let pending: Vec<usize> = shared
            .lock()
            .await
            .complete
            .iter()
            .enumerate()
            .filter_map(|(i, done)| (!done).then_some(i))
            .collect();
        if pending.is_empty() {
            break;
        }
        crate::debug_log!("download {}: scheduling {} chunks", journal.id, pending.len());
        ensure!(
            !routes.is_empty(),
            "all routes failed; transfer can be resumed later"
        );
        let queue = Arc::new(Mutex::new(VecDeque::from(pending)));
        let mut workers = tokio::task::JoinSet::new();
        for route in routes.iter().cloned() {
            let config = config.clone();
            let shared = shared.clone();
            let queue = queue.clone();
            workers.spawn(async move {
                let name = route.name.clone();
                let started = Instant::now();
                let result = get_worker(config, route, shared, queue).await;
                (name, started.elapsed(), result)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = workers.join_next().await {
            let (name, elapsed, outcome) = result?;
            note_worker_finished(&shared, &name, elapsed, outcome.is_err(), &control_sftp).await?;
            if let Err(error) = outcome {
                crate::debug::write_stderr(format_args!("route {name}: {error:#}"));
                note_error(&shared, &name, &error, &control_sftp).await?;
                let attempts = failures.entry(name.clone()).or_default();
                *attempts += 1;
                if !retry_route(&error, *attempts) {
                    failed.push(name);
                }
            }
        }
        routes.retain(|r| !failed.contains(&r.name));
        if !routes.is_empty() && shared.lock().await.complete.iter().any(|&done| !done) {
            let attempts = failures.values().copied().max().unwrap_or(0);
            tokio::time::sleep(Duration::from_secs(u64::from(attempts.min(2)))).await;
        }
    }
    *journal = shared.lock().await.clone();
    if journal.verify {
        let (size, sha256, _) = local_hash(Path::new(&journal.temporary)).await?;
        ensure!(size == journal.size && sha256 == journal.sha256,
            "downloaded SHA-256 differs; temporary file retained");
        crate::debug_log!("download {}: local SHA-256 verified", journal.id);
    } else {
        ensure!(fs::metadata(&journal.temporary)?.len() == journal.size,
            "downloaded file size differs; temporary file retained");
        let metadata = control_sftp.metadata(&journal.source).await?;
        let mtime = metadata.modified().ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|time| time.as_secs());
        ensure!(metadata.len() == journal.size && mtime == journal.source_mtime,
            "remote source changed during download; temporary file retained");
    }
    let destination = Path::new(&journal.destination);
    fs::hard_link(&journal.temporary, destination)
        .context("cannot publish without overwriting destination")?;
    journal.published = true;
    crate::debug_log!("download {}: published {}", journal.id, journal.destination);
    save(&control_sftp, journal).await?;
    let _ = fs::remove_file(&journal.temporary);
    Ok(())
}

async fn get_worker(
    config: Arc<Config>,
    route: Route,
    shared: Arc<Mutex<Journal>>,
    queue: Arc<Mutex<VecDeque<usize>>>,
) -> Result<()> {
    let connect_started = Instant::now();
    let connection = match transport::connect(&config, &route).await {
        Ok(connection) => connection,
        Err(error) => { note_failed_connection(&shared, &route.name, connect_started).await; return Err(error); }
    };
    let sftp = match sftp(&connection).await {
        Ok(sftp) => sftp,
        Err(error) => { note_failed_connection(&shared, &route.name, connect_started).await; return Err(error); }
    };
    note_session(&shared, &route.name, connect_started.elapsed()).await;
    let snapshot = shared.lock().await.clone();
    let metadata = sftp.metadata(&snapshot.source).await?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|t| t.as_secs());
    ensure!(
        metadata.len() == snapshot.size && mtime == snapshot.source_mtime,
        "remote source changed"
    );
    let mut remote = sftp.open(&snapshot.source).await?;
    let mut local = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&snapshot.temporary)
        .await?;
    loop {
        let selected_size = { let state = shared.lock().await; route_batch_size(&state, &route.name) };
        let max_atoms = (selected_size / snapshot.chunk_size).max(1);
        let group = claim_group(&mut *queue.lock().await, max_atoms);
        if group.is_empty() {
            break;
        }
        let chunk_started = Instant::now();
        let offset = group[0] as u64 * snapshot.chunk_size as u64;
        let length = (snapshot.size - offset).min(group.len() as u64 * snapshot.chunk_size as u64) as usize;
        let batch_io = async {
            remote.seek(SeekFrom::Start(offset)).await?;
            let mut buffer = vec![0u8; length];
            remote.read_exact(&mut buffer).await?;
            local.seek(SeekFrom::Start(offset)).await?;
            local.write_all(&buffer).await?;
            local.sync_data().await?;
            Ok::<(), anyhow::Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(snapshot.chunk_timeout_secs.unwrap_or(120)), batch_io).await;
        match result {
            Ok(Ok(())) => {},
            Ok(Err(error)) => { note_failed_batch(&shared, &route.name, selected_size, chunk_started).await; return Err(error); },
            Err(_) => {
                note_failed_batch(&shared, &route.name, selected_size, chunk_started).await;
                anyhow::bail!("batch starting at atom {} exceeded {} second timeout", group[0], snapshot.chunk_timeout_secs.unwrap_or(120));
            }
        }
        note_batch(&shared, &sftp, &route.name, &group, length, selected_size, chunk_started).await?;
    }
    remote.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_hash_path_is_shell_quoted() {
        assert_eq!(shell_quote("/tmp/a'b $(touch unsafe)"), "'/tmp/a'\\''b $(touch unsafe)'");
    }

    #[test]
    fn groups_only_adjacent_pending_atoms() {
        let mut pending = VecDeque::from(vec![0, 1, 4, 5, 6]);
        assert_eq!(claim_group(&mut pending, 4), [0, 1]);
        assert_eq!(claim_group(&mut pending, 2), [4, 5]);
        assert_eq!(claim_group(&mut pending, 2), [6]);
        assert!(claim_group(&mut pending, 2).is_empty());
    }

    #[test]
    fn adaptive_batches_use_medians_and_stop_after_a_slower_size() {
        let mib = 1024 * 1024;
        let mut stats = RouteTransferStats::default();
        for _ in 0..3 { observe_batch(&mut stats, mib, mib, Duration::from_millis(100), 256 * 1024, 8 * mib, 120); }
        assert_eq!(stats.chosen_batch_size, 2 * mib);
        for _ in 0..3 { observe_batch(&mut stats, 2 * mib, 2 * mib, Duration::from_millis(300), 256 * 1024, 8 * mib, 120); }
        assert_eq!(stats.chosen_batch_size, mib);
        assert_eq!(stats.adaptive_max_size, mib);
        for _ in 0..10 { observe_batch(&mut stats, mib, mib, Duration::from_millis(100), 256 * 1024, 8 * mib, 120); }
        assert_eq!(stats.chosen_batch_size, mib);
    }

    #[test]
    fn one_failure_does_not_shrink_but_two_do() {
        let mib = 1024 * 1024;
        let mut stats = RouteTransferStats { chosen_batch_size: 2 * mib, ..Default::default() };
        observe_batch(&mut stats, 2 * mib, 0, Duration::from_millis(500), 256 * 1024, 8 * mib, 120);
        assert_eq!(stats.chosen_batch_size, 2 * mib);
        observe_batch(&mut stats, 2 * mib, 2 * mib, Duration::from_millis(100), 256 * 1024, 8 * mib, 120);
        assert_eq!(stats.consecutive_failures, 0);
        observe_batch(&mut stats, 2 * mib, 0, Duration::from_millis(500), 256 * 1024, 8 * mib, 120);
        observe_batch(&mut stats, 2 * mib, 0, Duration::from_millis(500), 256 * 1024, 8 * mib, 120);
        assert_eq!(stats.chosen_batch_size, mib);
    }

    #[test]
    fn connection_time_is_part_of_first_batch_observation() {
        let mib = 1024 * 1024;
        let mut stats = RouteTransferStats { pending_connect_ms: 250, ..Default::default() };
        observe_batch(&mut stats, mib, mib, Duration::from_millis(100), 256 * 1024, 8 * mib, 120);
        assert_eq!(stats.observations[0].elapsed_ms, 350);
        assert_eq!(stats.pending_connect_ms, 0);
    }

    #[test]
    fn auto_size_respects_batch_timeout() {
        let mib = 1024 * 1024;
        let mut slow = RouteTransferStats { chosen_batch_size: 4 * mib, ..Default::default() };
        for _ in 0..3 {
            observe_batch(&mut slow, 4 * mib, 4 * mib, Duration::from_secs(70), 256 * 1024, 8 * mib, 120);
        }
        assert_eq!(slow.chosen_batch_size, 4 * mib);
        let mut fast = RouteTransferStats { chosen_batch_size: 4 * mib, ..Default::default() };
        for _ in 0..3 {
            observe_batch(&mut fast, 4 * mib, 4 * mib, Duration::from_secs(20), 256 * 1024, 8 * mib, 120);
        }
        assert_eq!(fast.chosen_batch_size, 8 * mib);
    }

    #[cfg(unix)]
    #[test]
    fn active_transfer_is_not_offered_for_a_second_worker() {
        let mut journal: Journal = serde_json::from_value(serde_json::json!({
            "id": Uuid::new_v4(), "direction": "Put", "target": "edge_a",
            "source": "/tmp/source", "destination": "/tmp/destination",
            "temporary": "/tmp/destination.part", "size": 1,
            "source_mtime": null, "sha256": "", "chunk_size": 65536,
            "complete": [false], "published": false
        })).unwrap();
        assert_eq!(journal.version, 0);
        assert!(journal.verify);
        journal.worker_host = worker_host();
        journal.worker_pid = std::process::id();
        journal.updated_at_unix_ms = now_unix_ms();
        assert!(is_active(&journal));
        journal.worker_pid = u32::MAX;
        assert!(!is_active(&journal));
    }

    #[test]
    fn transient_failures_get_bounded_retries() {
        let timeout = anyhow::anyhow!("connection timed out");
        assert!(retry_route(&timeout, 1));
        assert!(retry_route(&timeout, 2));
        assert!(!retry_route(&timeout, 3));
        assert!(!retry_route(&anyhow::Error::new(russh::Error::UnknownKey), 1));
    }
}
