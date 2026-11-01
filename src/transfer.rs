use crate::{
    config::{Config, Route},
    transport,
};
use anyhow::{Context, Result, ensure};
use russh_sftp::{client::SftpSession, protocol::OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

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
    pub owner_pid: u32,
}

pub fn is_active(journal: &Journal) -> bool {
    if journal.published || journal.owner_pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        let Ok(pid) = i32::try_from(journal.owner_pid) else {
            return false;
        };
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        false
    }
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

fn note_session(shared: &Arc<Mutex<Journal>>, route: &str) -> Result<()> {
    let mut journal = shared.lock().unwrap();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    if stats.sessions > 0 {
        stats.reconnects = stats.reconnects.saturating_add(1);
    }
    stats.sessions = stats.sessions.saturating_add(1);
    save(&journal)
}

fn note_error(shared: &Arc<Mutex<Journal>>, route: &str) -> Result<()> {
    let mut journal = shared.lock().unwrap();
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.errors = stats.errors.saturating_add(1);
    save(&journal)
}

fn note_chunk(
    shared: &Arc<Mutex<Journal>>,
    route: &str,
    index: usize,
    bytes: usize,
    elapsed: Duration,
) -> Result<()> {
    let mut journal = shared.lock().unwrap();
    journal.complete[index] = true;
    let stats = journal.route_stats.entry(route.to_owned()).or_default();
    stats.bytes = stats.bytes.saturating_add(bytes as u64);
    let sample_bps = bytes as f64 / elapsed.as_secs_f64().max(0.001);
    stats.last_bps = Some(
        stats
            .last_bps
            .map_or(sample_bps, |old| old * 0.7 + sample_bps * 0.3),
    );
    stats.last_at_unix_ms = now_unix_ms();
    save(&journal)?;
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

fn state_dir() -> Result<PathBuf> {
    Ok(dirs::state_dir()
        .or_else(|| dirs::home_dir().map(|p| p.join(".local/state")))
        .context("cannot find state directory")?
        .join("mssh/transfers"))
}

fn journal_path(id: Uuid) -> Result<PathBuf> {
    Ok(state_dir()?.join(format!("{id}.json")))
}

fn save(journal: &Journal) -> Result<()> {
    let directory = state_dir()?;
    fs::create_dir_all(&directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    }
    let path = journal_path(journal.id)?;
    let temporary = path.with_extension(format!("json.tmp-{}", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(journal)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

pub fn load(id: Uuid) -> Result<Journal> {
    let path = journal_path(id)?;
    Ok(serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("cannot load transfer {id}"))?,
    )?)
}

pub fn list() -> Result<Vec<Journal>> {
    let directory = state_dir()?;
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut journals = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json")
            && let Ok(journal) = serde_json::from_slice(&fs::read(path)?)
        {
            journals.push(journal);
        }
    }
    journals.sort_by_key(|j: &Journal| j.id);
    Ok(journals)
}

pub async fn cancel(config: &Config, id: Uuid) -> Result<()> {
    let journal = load(id)?;
    ensure!(!journal.published, "cannot cancel a published transfer");
    ensure!(!is_active(&journal), "transfer is still active");
    match journal.direction {
        Direction::Put => {
            let connection = connect_any(config, &journal.target).await?;
            let sftp = sftp(&connection).await?;
            ensure!(
                !sftp.try_exists(&journal.destination).await?,
                "destination already exists; inspect it before cancellation"
            );
            if sftp.try_exists(&journal.temporary).await? {
                sftp.remove_file(&journal.temporary).await?;
            }
        }
        Direction::Get => {
            ensure!(
                !Path::new(&journal.destination).exists(),
                "destination already exists; inspect it before cancellation"
            );
            if Path::new(&journal.temporary).exists() {
                fs::remove_file(&journal.temporary)?;
            }
        }
    }
    fs::remove_file(journal_path(id)?)?;
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
        match transport::connect(config, route).await {
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
        owner_pid: std::process::id(),
    };
    let connection = connect_any(config, target).await?;
    let sftp = sftp(&connection).await?;
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
    save(&journal)?;
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
        .cloned()
        .collect();
    ensure!(!routes.is_empty(), "no route to target {}", journal.target);
    let shared = Arc::new(Mutex::new(journal.clone()));
    let config = Arc::new(config.clone());
    loop {
        let pending: Vec<usize> = shared
            .lock()
            .unwrap()
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
                note_error(&shared, &name)?;
                failed.push(name);
            }
        }
        routes.retain(|r| !failed.contains(&r.name));
    }
    *journal = shared.lock().unwrap().clone();
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
    save(journal)?;
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
    note_session(&shared, &route.name)?;
    let snapshot = shared.lock().unwrap().clone();
    let mut local = tokio::fs::File::open(&snapshot.source).await?;
    loop {
        let Some(index) = queue.lock().unwrap().pop_front() else {
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
        note_chunk(&shared, &route.name, index, length, chunk_started.elapsed())?;
    }
    Ok(())
}

pub async fn resume(config: &Config, id: Uuid) -> Result<()> {
    let mut journal = load(id)?;
    ensure!(!journal.published, "transfer already published");
    ensure!(!is_active(&journal), "transfer is still active");
    journal.owner_pid = std::process::id();
    // A chunk marked complete may have been changed after a crash. Rewriting the
    // immutable source bytes is safe and makes the final digest meaningful.
    journal.complete.fill(false);
    save(&journal)?;
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
        owner_pid: std::process::id(),
    };
    save(&journal)?;
    process_get(config, &mut journal).await?;
    Ok(id)
}

async fn process_get(config: &Config, journal: &mut Journal) -> Result<()> {
    let mut routes: Vec<Route> = config
        .routes_for(&journal.target)
        .into_iter()
        .take(config.settings.max_parallel_routes)
        .cloned()
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
    let shared = Arc::new(Mutex::new(journal.clone()));
    let config = Arc::new(config.clone());
    loop {
        let pending: Vec<usize> = shared
            .lock()
            .unwrap()
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
                note_error(&shared, &name)?;
                failed.push(name);
            }
        }
        routes.retain(|r| !failed.contains(&r.name));
    }
    *journal = shared.lock().unwrap().clone();
    let (size, sha256, _) = local_hash(Path::new(&journal.temporary)).await?;
    ensure!(
        size == journal.size && sha256 == journal.sha256,
        "downloaded SHA-256 differs; temporary file retained"
    );
    let destination = Path::new(&journal.destination);
    fs::hard_link(&journal.temporary, destination)
        .context("cannot publish without overwriting destination")?;
    journal.published = true;
    save(journal)?;
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
    note_session(&shared, &route.name)?;
    let snapshot = shared.lock().unwrap().clone();
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
        let Some(index) = queue.lock().unwrap().pop_front() else {
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
        note_chunk(&shared, &route.name, index, length, chunk_started.elapsed())?;
    }
    remote.close().await?;
    Ok(())
}
