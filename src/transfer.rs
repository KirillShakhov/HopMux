use crate::{
    config::{Config, Route},
    transport,
};
use anyhow::{Context, Result, ensure};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Journal {
    pub id: Uuid,
    pub direction: Direction,
    pub target: String,
    pub source: String,
    pub destination: String,
    pub temporary: String,
    pub size: u64,
    pub source_mtime: Option<u64>,
    pub sha256: String,
    pub chunk_size: usize,
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
    now_unix_ms().saturating_sub(journal.updated_at_unix_ms) < 30_000
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouteTransferStats {
    pub bytes: u64,
    pub last_bps: Option<f64>,
    pub last_at_unix_ms: u128,
    pub errors: u64,
    pub reconnects: u64,
    pub sessions: u64,
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

async fn note_session(shared: &Arc<Mutex<Journal>>, route: &str, sftp: &SftpSession) -> Result<()> {
    let mut journal = shared.lock().await;
    journal.updated_at_unix_ms = now_unix_ms();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    if stats.sessions > 0 {
        stats.reconnects = stats.reconnects.saturating_add(1);
    }
    stats.sessions = stats.sessions.saturating_add(1);
    save(sftp, &journal).await
}

async fn note_error(shared: &Arc<Mutex<Journal>>, route: &str, error: &anyhow::Error, sftp: &SftpSession) -> Result<()> {
    let mut journal = shared.lock().await;
    journal.updated_at_unix_ms = now_unix_ms();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.errors = stats.errors.saturating_add(1);
    journal.last_error = Some(format!("{error:#}"));
    save(sftp, &journal).await
}

fn retry_route(error: &anyhow::Error, attempts: u32) -> bool {
    !transport::is_permanent(error) && attempts < MAX_ROUTE_ATTEMPTS
}

async fn note_chunk(
    shared: &Arc<Mutex<Journal>>,
    sftp: &SftpSession,
    route: &str,
    index: usize,
    bytes: usize,
    elapsed: Duration,
) -> Result<()> {
    let mut journal = shared.lock().await;
    journal.complete[index] = true;
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
    save(sftp, &journal).await?;
    eprintln!(
        "{}: {}/{} chunks via {}",
        journal.id,
        journal.complete.iter().filter(|&&done| done).count(),
        journal.complete.len(),
        route
    );
    Ok(())
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
    Ok(())
}

async fn remove_stale_journal_writes(sftp: &SftpSession, journal: &Journal) -> Result<()> {
    let prefix = format!("{}.part.json.new-", journal.id);
    for entry in sftp.read_dir(&journal.state_dir).await? {
        let name = entry.file_name();
        if name.starts_with(&prefix) {
            sftp.remove_file(format!("{}/{name}", journal.state_dir)).await?;
        }
    }
    Ok(())
}

pub async fn list(config: &Config, target: &str) -> Result<Vec<Journal>> {
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
    Ok(journals.into_values().collect())
}

pub async fn discard(config: &Config, journal: &Journal) -> Result<()> {
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
    let channel = connection.final_handle().channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    Ok(SftpSession::new(channel.into_stream()).await?)
}

pub(crate) async fn connect_any(config: &Config, target: &str) -> Result<transport::Connection> {
    let mut last_error = None;
    for route in config.routes_for(target) {
        match transport::connect(config, &route).await {
            Ok(connection) => return Ok(connection),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no route to target {target}")))
}

async fn local_hash(path: &Path) -> Result<(u64, String, Option<u64>)> {
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
    Ok((metadata.len(), hex::encode(hash.finalize()), modified))
}

pub async fn put(config: &Config, target: &str, source: &Path, destination: &str) -> Result<Uuid> {
    let routes = config.routes_for(target);
    ensure!(!routes.is_empty(), "no route to target {target}");
    ensure!(
        destination.starts_with('/'),
        "remote destination must be an absolute path"
    );
    let (size, sha256, source_mtime) = local_hash(source).await?;
    let id = Uuid::new_v4();
    let temporary = format!("{destination}.mssh-{id}.part");
    let chunks = size.div_ceil(config.settings.chunk_size as u64) as usize;
    let connection = connect_any(config, target).await?;
    let sftp = sftp(&connection).await?;
    let mut journal = Journal {
        id,
        direction: Direction::Put,
        target: target.into(),
        source: source.to_string_lossy().into_owned(),
        destination: destination.into(),
        temporary,
        size,
        source_mtime,
        sha256,
        chunk_size: config.settings.chunk_size,
        complete: vec![false; chunks],
        published: false,
        route_stats: BTreeMap::new(),
        last_error: None,
        state_dir: state_dir(&sftp).await?,
        worker_host: worker_host(),
        worker_pid: std::process::id(),
        updated_at_unix_ms: now_unix_ms(),
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
    if let Err(error) = save(&sftp, &journal).await {
        let _ = sftp.remove_file(&journal.temporary).await;
        return Err(error);
    }
    process_put(config, &mut journal).await?;
    Ok(id)
}

async fn process_put(config: &Config, journal: &mut Journal) -> Result<()> {
    let source = Path::new(&journal.source);
    let (size, sha256, mtime) = local_hash(source).await?;
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
    let control_connection = connect_any(config, &journal.target).await?;
    let control_sftp = sftp(&control_connection).await?;
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
                (name, put_worker(config, route, shared, queue).await)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = workers.join_next().await {
            let (name, outcome) = result?;
            if let Err(error) = outcome {
                eprintln!("route {name}: {error:#}");
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
    let connection = connect_any(&config, &journal.target).await?;
    let sftp = sftp(&connection).await?;
    let mut remote = sftp.open(&journal.temporary).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; journal.chunk_size.min(1024 * 1024)];
    loop {
        let count = remote.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    remote.close().await?;
    ensure!(
        hex::encode(hash.finalize()) == journal.sha256,
        "remote SHA-256 differs; temporary file retained"
    );
    ensure!(
        !sftp.try_exists(&journal.destination).await?,
        "destination appeared during upload"
    );
    let linked = sftp
        .hardlink(&journal.temporary, &journal.destination)
        .await?;
    ensure!(
        linked,
        "server lacks hardlink@openssh.com; verified temporary file retained at {}",
        journal.temporary
    );
    journal.published = true;
    save(&sftp, journal).await?;
    // The published name is safe now; cleanup failure is nonfatal.
    let _ = sftp.remove_file(&journal.temporary).await;
    Ok(())
}

async fn put_worker(
    config: Arc<Config>,
    route: Route,
    shared: Arc<Mutex<Journal>>,
    queue: Arc<Mutex<VecDeque<usize>>>,
) -> Result<()> {
    let connection = transport::connect(&config, &route).await?;
    let sftp = sftp(&connection).await?;
    note_session(&shared, &route.name, &sftp).await?;
    let snapshot = shared.lock().await.clone();
    let mut local = tokio::fs::File::open(&snapshot.source).await?;
    loop {
        let Some(index) = queue.lock().await.pop_front() else {
            break;
        };
        let chunk_started = Instant::now();
        let offset = index as u64 * snapshot.chunk_size as u64;
        let length = (snapshot.size - offset).min(snapshot.chunk_size as u64) as usize;
        let mut buffer = vec![0u8; length];
        local.seek(SeekFrom::Start(offset)).await?;
        local.read_exact(&mut buffer).await?;
        let mut remote = sftp
            .open_with_flags(&snapshot.temporary, OpenFlags::WRITE)
            .await?;
        remote.seek(SeekFrom::Start(offset)).await?;
        remote.write_all(&buffer).await?;
        remote.close().await?;
        let mut check = sftp.open(&snapshot.temporary).await?;
        check.seek(SeekFrom::Start(offset)).await?;
        let mut readback = vec![0u8; length];
        check.read_exact(&mut readback).await?;
        check.close().await?;
        ensure!(
            Sha256::digest(&buffer) == Sha256::digest(&readback),
            "remote chunk {index} differs after write"
        );
        note_chunk(&shared, &sftp, &route.name, index, length, chunk_started.elapsed()).await?;
    }
    Ok(())
}

pub async fn continue_transfer(config: &Config, journal: &Journal) -> Result<()> {
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
        Direction::Put => process_put(config, &mut journal).await,
        Direction::Get => process_get(config, &mut journal).await,
    }
}

pub async fn get(config: &Config, target: &str, source: &str, destination: &Path) -> Result<Uuid> {
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
    let mut remote = sftp.open(source).await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; config.settings.chunk_size.min(1024 * 1024)];
    loop {
        let n = remote.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    remote.close().await?;
    let id = Uuid::new_v4();
    let temporary = format!("{}.mssh-{id}.part", destination.display());
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(&temporary)?;
    let mut journal = Journal {
        id,
        direction: Direction::Get,
        target: target.into(),
        source: source.into(),
        destination: destination.to_string_lossy().into_owned(),
        temporary,
        size,
        source_mtime,
        sha256: hex::encode(hash.finalize()),
        chunk_size: config.settings.chunk_size,
        complete: vec![false; size.div_ceil(config.settings.chunk_size as u64) as usize],
        published: false,
        route_stats: BTreeMap::new(),
        last_error: None,
        state_dir: state_dir(&sftp).await?,
        worker_host: worker_host(),
        worker_pid: std::process::id(),
        updated_at_unix_ms: now_unix_ms(),
    };
    if let Err(error) = save(&sftp, &journal).await {
        let _ = fs::remove_file(&journal.temporary);
        return Err(error);
    }
    process_get(config, &mut journal).await?;
    Ok(id)
}

async fn process_get(config: &Config, journal: &mut Journal) -> Result<()> {
    let mut routes: Vec<Route> = config
        .routes_for(&journal.target)
        .into_iter()
        .take(config.settings.max_parallel_routes)
        .collect();
    ensure!(!routes.is_empty(), "no route to target {}", journal.target);
    ensure!(
        !Path::new(&journal.destination).exists(),
        "local destination already exists"
    );
    ensure!(
        Path::new(&journal.temporary).exists(),
        "local temporary file is missing"
    );
    let control_connection = connect_any(config, &journal.target).await?;
    let control_sftp = sftp(&control_connection).await?;
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
                (name, get_worker(config, route, shared, queue).await)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = workers.join_next().await {
            let (name, outcome) = result?;
            if let Err(error) = outcome {
                eprintln!("route {name}: {error:#}");
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
    let (size, sha256, _) = local_hash(Path::new(&journal.temporary)).await?;
    ensure!(
        size == journal.size && sha256 == journal.sha256,
        "downloaded SHA-256 differs; temporary file retained"
    );
    let destination = Path::new(&journal.destination);
    fs::hard_link(&journal.temporary, destination)
        .context("cannot publish without overwriting destination")?;
    journal.published = true;
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
    let connection = transport::connect(&config, &route).await?;
    let sftp = sftp(&connection).await?;
    note_session(&shared, &route.name, &sftp).await?;
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
        let Some(index) = queue.lock().await.pop_front() else {
            break;
        };
        let chunk_started = Instant::now();
        let offset = index as u64 * snapshot.chunk_size as u64;
        let length = (snapshot.size - offset).min(snapshot.chunk_size as u64) as usize;
        remote.seek(SeekFrom::Start(offset)).await?;
        let mut buffer = vec![0u8; length];
        remote.read_exact(&mut buffer).await?;
        local.seek(SeekFrom::Start(offset)).await?;
        local.write_all(&buffer).await?;
        local.sync_data().await?;
        note_chunk(&shared, &sftp, &route.name, index, length, chunk_started.elapsed()).await?;
    }
    remote.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
