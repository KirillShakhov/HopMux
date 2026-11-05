use crate::{config::{Config, Route}, terminal, transport, transfer, directory};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::{path::{Path, PathBuf}, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::AsyncWriteExt, sync::Semaphore};

#[derive(Serialize)]
pub struct AuthStatus {
    pub node: String,
    pub key_available: bool,
    pub password_available: bool,
    pub agent_available: bool,
    pub usable: bool,
}

pub fn auth_statuses(config: &Config) -> Vec<AuthStatus> {
    config.nodes.iter().map(|(id, node)| {
        let key_available = node.key_file.as_ref().is_some_and(|path| {
            let expanded = if let Some(rest) = path.strip_prefix("~/") {
                dirs::home_dir().map(|home| home.join(rest))
            } else { Some(PathBuf::from(path)) };
            expanded.is_some_and(|path| path.is_file())
        });
        let password_available = node.password.is_some() || node.password_env.as_ref()
            .is_some_and(|name| std::env::var_os(name).is_some());
        let agent_available = node.agent && cfg!(unix) && std::env::var_os("SSH_AUTH_SOCK")
            .is_some_and(|path| Path::new(&path).exists());
        let usable = key_available || password_available || agent_available;
        crate::debug_log!("status: auth {} key={} password={} agent={} usable={}", id, key_available, password_available, agent_available, usable);
        AuthStatus { node: id.clone(), key_available, password_available, agent_available, usable }
    }).collect()
}

#[derive(Clone, Serialize)]
pub struct RouteStatus {
    pub route: String,
    pub target: String,
    pub state: String,
    pub connect_ms: Option<u128>,
    #[serde(default)]
    pub rtt_ms: Option<u128>,
    #[serde(default)]
    pub checked_at_unix_ms: u128,
    pub error: Option<String>,
    pub hops: Vec<String>,
    #[serde(default)]
    pub hop_timings: Vec<transport::HopTiming>,
}


pub async fn probe_routes(config: &Config, target: Option<&str>) -> Vec<RouteStatus> {
    let routes: Vec<Route> = config
        .effective_routes()
        .into_iter()
        .filter(|r| !r.disabled && target.is_none_or(|t| r.target == t)
            && (config.route_filter.is_empty() || config.route_filter.contains(&r.name)))
        .collect();
    let shared = Arc::new(config.clone());
    let semaphore = Arc::new(Semaphore::new(config.settings.max_parallel_routes));
    let mut tasks = Vec::new();
    crate::debug_log!("status: probing {} enabled routes", routes.len());
    for route in routes {
        let shared = shared.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            let result = transport::connect(&shared, &route).await;
            crate::debug_log!("status: route {} probe {}", route.name, if result.is_ok() { "connected" } else { "failed" });
            let checked_at_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            match result {
                Ok(conn) => {
                    let start = Instant::now();
                    let ping = tokio::time::timeout(
                        Duration::from_secs(shared.settings.connect_timeout_secs),
                        conn.final_handle().send_ping(),
                    )
                    .await;
                    let (state, rtt_ms, error) = match ping {
                        Ok(Ok(())) => ("UP".to_owned(), Some(start.elapsed().as_millis()), None),
                        Ok(Err(error)) => (
                            "STALLED".to_owned(),
                            None,
                            Some(format!("SSH ping failed: {error}")),
                        ),
                        Err(_) => (
                            "STALLED".to_owned(),
                            None,
                            Some("SSH ping timed out".to_owned()),
                        ),
                    };
                    crate::debug_log!("status: route {} state {}, RTT {:?} ms", route.name, state, rtt_ms);
                    RouteStatus {
                        route: route.name,
                        target: route.target,
                        state,
                        connect_ms: Some(conn.elapsed.as_millis()),
                        rtt_ms,
                        checked_at_unix_ms,
                        error,
                        hops: route.hops,
                        hop_timings: conn.hop_timings,

                    }
                }
                Err(error) => {
                    crate::debug_log!("status: route {} failed: {error:#}", route.name);
                    RouteStatus {
                    route: route.name,
                    target: route.target,
                    state: transport::failure_state(&error).into(),
                    connect_ms: None,
                    rtt_ms: None,
                    checked_at_unix_ms,
                    error: Some(format!("{error:#}")),
                    hops: route.hops,
                    hop_timings: Vec::new(),

                    }
                },
            }
        }));
    }
    let mut statuses = Vec::new();
    for task in tasks {
        if let Ok(status) = task.await {
            statuses.push(status);
        }
    }
    statuses
}

pub async fn ssh(config: &Config, target: &str) -> Result<()> {
    let mut routes = config.routes_for(target);
    if routes.is_empty() {
        bail!("no route to target {target}");
    }
    let mut index = 0;
    loop {
        let route = routes[index % routes.len()].clone();
        crate::debug_log!("SSH shell: trying route {} (attempt {})", route.name, index + 1);
        index += 1;
        match transport::connect(config, &route).await {
            Ok(connection) => {
                eprintln!("connected via {} (Ctrl-] to reconnect)", route.name);
                match shell(&connection).await {
                    Ok(true) => { crate::debug_log!("SSH shell: remote session ended normally"); return Ok(()) },
                    Ok(false) => { crate::debug_log!("SSH shell: reconnect requested"); eprintln!("reconnecting...") },
                    Err(error) => { crate::debug_log!("SSH shell: disconnected: {error:#}"); eprintln!("shell disconnected: {error:#}; reconnecting...") },
                }
            }
            Err(error) => {
                crate::debug_log!("SSH shell: route {} failed: {error:#}", route.name);
                eprintln!("route {} failed: {error:#}", route.name);
                if transport::is_permanent(&error) {
                    routes.retain(|candidate| candidate.name != route.name);
                    if routes.is_empty() {
                        bail!(
                            "all routes to {target} have permanent authentication or host-key errors"
                        );
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn shell(connection: &transport::Connection) -> Result<bool> {
    crate::debug_log!("SSH shell: opening session channel");
    let channel = connection.final_handle().channel_open_session().await?;
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let term = std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".into());
    channel
        .request_pty(true, &term, cols as u32, rows as u32, 0, 0, &[])
        .await?;
    channel.request_shell(true).await?;
    crate::debug_log!("SSH shell: PTY requested, terminal {}x{}", cols, rows);
    crossterm::terminal::enable_raw_mode().context("cannot enable raw terminal")?;
    let result = shell_io(channel, (cols, rows)).await;
    crossterm::terminal::disable_raw_mode()?;
    match result? {
        ShellEnd::InputEnded => Ok(true),
        ShellEnd::ManualReconnect => Ok(false),
        ShellEnd::RemoteClosed { exited } => Ok(exited),
    }
}

enum ShellEnd {
    InputEnded,
    ManualReconnect,
    RemoteClosed { exited: bool },
}

async fn shell_io(
    channel: russh::Channel<russh::client::Msg>,
    mut size: (u16, u16),
) -> Result<ShellEnd> {
    let (mut remote_read, remote_write) = channel.split();
    let mut remote_input = remote_write.make_writer();
    let mut stdin = terminal::Input::new()?;
    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut input = [0u8; 4096];
    let mut exited = false;
    let mut resize = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            count = stdin.read(&mut input) => {
                let count = count?;
                if count == 0 { return Ok(ShellEnd::InputEnded); }
                if input[..count].contains(&0x1d) { return Ok(ShellEnd::ManualReconnect); }
                remote_input.write_all(&input[..count]).await?;
            }
            message = remote_read.wait() => {
                match message {
                    Some(russh::ChannelMsg::Data { data }) => { stdout.write_all(&data).await?; stdout.flush().await?; }
                    Some(russh::ChannelMsg::ExtendedData { data, .. }) => { stderr.write_all(&data).await?; stderr.flush().await?; }
                    Some(russh::ChannelMsg::ExitStatus { .. } | russh::ChannelMsg::ExitSignal { .. }) => exited = true,
                    Some(russh::ChannelMsg::Close) | None => return Ok(ShellEnd::RemoteClosed { exited }),
                    _ => {}
                }
            }
            _ = resize.tick() => {
                if let Ok(next) = crossterm::terminal::size() && next != size {
                    size = next;
                    remote_write.window_change(size.0 as u32, size.1 as u32, 0, 0).await?;
                }
            }
        }
    }
}

pub enum CopySource {
    Local(PathBuf),
    Remote(String),
}

pub struct CopyPlan {
    pub target: String,
    pub source: CopySource,
    pub destination: String,
    pub partial: Option<transfer::Journal>,
    pub recursive: bool,
}

pub enum PartialAction {
    Continue,
    Restart,
    New,
}

fn remote_spec<'a>(config: &Config, arg: &'a str) -> Result<Option<(&'a str, &'a str)>> {
    let Some((target, path)) = arg.split_once(':') else { return Ok(None) };
    if !config.nodes.contains_key(target) {
        return Ok(None);
    }
    ensure!(path.starts_with('/'), "remote path must be absolute: {arg}");
    Ok(Some((target, path)))
}

pub async fn prepare_copy(config: &Config, source: &str, destination: &str, recursive: bool) -> Result<CopyPlan> {
    let from = remote_spec(config, source)?;
    let to = remote_spec(config, destination)?;
    crate::debug_log!("copy: source remote={}, destination remote={}, recursive={recursive}", from.is_some(), to.is_some());
    let (target, source, destination) = match (from, to) {
        (Some((target, remote)), None) => {
            let name = Path::new(remote).file_name().and_then(|n| n.to_str()).context("invalid remote filename")?;
            let local = Path::new(destination);
            let local = if local.is_dir() { local.join(name) } else { local.to_path_buf() };
            let parent = local.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
            let local = std::fs::canonicalize(parent)?.join(local.file_name().context("invalid destination")?);
            crate::debug_log!("copy: download {}:{} -> {}", target, remote, local.display());
            (target.to_owned(), CopySource::Remote(remote.to_owned()), local.to_string_lossy().into_owned())
        }
        (None, Some((target, remote))) => {
            let local = std::fs::canonicalize(source)?;
            let name = local.file_name().and_then(|n| n.to_str()).context("invalid local filename")?;
            let connection = transfer::connect_any(config, target).await?;
            let sftp = transfer::sftp(&connection).await?;
            let remote = if sftp.try_exists(remote).await? && sftp.metadata(remote).await?.file_type().is_dir() {
                format!("{}/{}", remote.trim_end_matches('/'), name)
            } else { remote.to_owned() };
            crate::debug_log!("copy: upload {} -> {}:{}", local.display(), target, remote);
            (target.to_owned(), CopySource::Local(local), remote)
        }
        _ => bail!("specify one local and one TARGET:/absolute/path endpoint"),
    };
    let journals = transfer::list(config, &target).await?;
    crate::debug_log!("copy: checked {} remote transfer journals", journals.len());
    let partial = journals.into_iter().find(|j| {
        !j.published && j.destination == destination && match &source {
            CopySource::Local(p) => matches!(j.direction, transfer::Direction::Put) && j.source.as_str() == p.to_string_lossy().as_ref(),
            CopySource::Remote(p) => matches!(j.direction, transfer::Direction::Get) && j.source.as_str() == p.as_str(),
        }
    });
    if let Some(journal) = &partial {
        crate::debug_log!("copy: matching partial transfer {} has {}/{} chunks", journal.id, journal.complete.iter().filter(|&&done| done).count(), journal.complete.len());
        ensure!(!transfer::is_active(journal), "transfer is still active; inspect it with mssh transfers {target}");
    }
    Ok(CopyPlan { target, source, destination, partial, recursive })
}

pub async fn execute_copy(config: &Config, plan: CopyPlan, action: PartialAction) -> Result<()> {
    if let Some(journal) = &plan.partial {
        match action {
            PartialAction::Continue => { crate::debug_log!("copy: continuing transfer {}", journal.id); return transfer::continue_transfer(config, journal).await },
            PartialAction::Restart => { crate::debug_log!("copy: discarding partial transfer {}", journal.id); transfer::discard(config, journal).await? },
            PartialAction::New => bail!("partial transfer exists"),
        }
    }
    match plan.source {
        CopySource::Local(path) => {
            if path.is_dir() {
                crate::debug_log!("copy: uploading directory {}", path.display());
                ensure!(plan.recursive, "source is a directory; use -r");
                directory::put_directory(config, &plan.target, &path, &plan.destination).await
            } else {
                crate::debug_log!("copy: uploading file {}", path.display());
                transfer::put(config, &plan.target, &path, &plan.destination).await?;
                Ok(())
            }
        }
        CopySource::Remote(path) => {
            let connection = transfer::connect_any(config, &plan.target).await?;
            let sftp = transfer::sftp(&connection).await?;
            if sftp.metadata(&path).await?.file_type().is_dir() {
                crate::debug_log!("copy: downloading directory {path}");
                ensure!(plan.recursive, "source is a directory; use -r");
                directory::get_directory(config, &plan.target, &path, Path::new(&plan.destination)).await
            } else {
                crate::debug_log!("copy: downloading file {path}");
                transfer::get(config, &plan.target, &path, Path::new(&plan.destination)).await?;
                Ok(())
            }
        }
    }
}
