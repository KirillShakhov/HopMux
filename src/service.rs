use crate::{config::{Config, Route}, terminal, transport, transfer, directory, output};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use std::{path::{Path, PathBuf}, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::AsyncWriteExt, sync::{Semaphore, oneshot}, time::MissedTickBehavior};

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
            crate::debug_log!("status: route {} probe {}", output::route_label(&route), if result.is_ok() { "connected" } else { "failed" });
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
                    crate::debug_log!("status: route {} state {}, RTT {}", output::route_label(&route), state, output::optional_ms(rtt_ms));
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
                    crate::debug_log!("status: route {} failed: {error:#}", output::route_label(&route));
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

pub async fn ssh(config: &Config, target: &str, options: ShellOptions) -> Result<()> {
    let mut routes = config.routes_for(target);
    if routes.is_empty() {
        bail!("no route to target {target}");
    }
    let mut index = 0;
    let session_name = format!("mssh-{}", uuid::Uuid::new_v4().simple());
    let mut mode = None;
    let mut raw_terminal = None;
    let mut stdin = None;
    let mut selected_connection: Option<(Route, Arc<transport::Connection>)> = None;
    loop {
        let (route, connection) = if let Some(selected) = selected_connection.take() {
            selected
        } else {
            let route = routes[index % routes.len()].clone();
            crate::debug_log!("SSH shell: trying route {} (attempt {})", output::route_label(&route), index + 1);
            index += 1;
            match transport::connect(config, &route).await {
                Ok(connection) => (route, Arc::new(connection)),
                Err(error) => {
                    crate::debug_log!("SSH shell: route {} failed: {error:#}", output::route_label(&route));
                    if transport::is_permanent(&error) {
                        routes.retain(|candidate| candidate.name != route.name);
                        if routes.is_empty() {
                            bail!("all routes to {target} have permanent authentication or host-key errors");
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        };
                let selected_mode = match mode {
                    Some(mode) => mode,
                    None => {
                        let tmux = if matches!(options.preference, ShellPreference::Auto | ShellPreference::Tmux) {
                            remote_command_ok(&connection, "command -v tmux >/dev/null 2>&1").await.unwrap_or(false)
                        } else { false };
                        let screen = if matches!(options.preference, ShellPreference::Screen)
                            || matches!(options.preference, ShellPreference::Auto) && !tmux {
                            remote_command_ok(&connection, "command -v screen >/dev/null 2>&1").await.unwrap_or(false)
                        } else { false };
                        let selected = choose_shell_mode(options.preference, tmux, screen);
                        crate::debug_log!("SSH shell: server-side mode {selected:?}");
                        mode = Some(selected);
                        selected
                    }
                };
                if raw_terminal.is_none() {
                    raw_terminal = Some(RawTerminal::new()?);
                    stdin = Some(terminal::Input::new()?);
                }
                crate::debug_log!("SSH shell: attached via {}", output::route_label(&route));
                match shell(connection, selected_mode, &session_name, stdin.as_mut().unwrap(),
                    config.clone(), routes.clone(), route.clone(), options).await {
                    Ok(ShellEnd::InputEnded | ShellEnd::RemoteClosed { exited: true }) => {
                        crate::debug_log!("SSH shell: remote session ended normally"); return Ok(());
                    }
                    Ok(ShellEnd::AutoSwitch(next_route, next_connection)) => {
                        crate::debug_log!("SSH shell: switching {} -> {}", output::route_label(&route), output::route_label(&next_route));
                        selected_connection = Some((next_route, next_connection));
                        continue;
                    }
                    Ok(ShellEnd::ManualReconnect) => {
                        crate::debug_log!("SSH shell: reconnect requested on {}", output::route_label(&route));
                    }
                    Ok(ShellEnd::RemoteClosed { exited: false }) => {
                        crate::debug_log!("SSH shell: {} disconnected", output::route_label(&route));
                    }
                    Err(error) => { crate::debug_log!("SSH shell: {} disconnected: {error:#}", output::route_label(&route)); },
                }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShellMode { Tmux, Screen, Plain }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShellPreference { Auto, Tmux, Screen, Plain }

#[derive(Clone, Copy)]
pub struct ShellOptions {
    pub preference: ShellPreference,
    pub min_rate_bytes_per_sec: u64,
    pub max_rtt: Duration,
    pub check_interval: Option<Duration>,
}

const INTERACTIVE_PROBE_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug)]
struct InteractiveQuality {
    rtt: Option<Duration>,
    bytes_per_second: Option<u64>,
}

impl InteractiveQuality {
    fn sufficient(self, options: ShellOptions) -> bool {
        self.rtt.is_some_and(|rtt| rtt <= options.max_rtt)
            && self.bytes_per_second.is_some_and(|rate| rate >= options.min_rate_bytes_per_sec)
    }

    fn score(self) -> u128 {
        let rtt_ms = self.rtt.map_or(60_000, |rtt| rtt.as_millis());
        let screen_ms = self.bytes_per_second.filter(|rate| *rate > 0)
            .map_or(60_000, |rate| INTERACTIVE_PROBE_BYTES as u128 * 1000 / rate as u128);
        rtt_ms.saturating_add(screen_ms)
    }
}

fn better_shell_route(current: InteractiveQuality, candidate: InteractiveQuality, options: ShellOptions) -> bool {
    if candidate.sufficient(options) { return true; }
    candidate.score().saturating_mul(4) < current.score().saturating_mul(3)
}

type ShellSwitch = (Route, Arc<transport::Connection>);

fn monitor_shell_route(
    config: Config,
    routes: Vec<Route>,
    current_route: Route,
    current_connection: Arc<transport::Connection>,
    options: ShellOptions,
    switch_tx: oneshot::Sender<ShellSwitch>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval = options.check_interval?;
    Some(tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut poor_samples = 0;
        loop {
            ticks.tick().await;
            let current = measure_interactive_quality(&current_connection).await;
            crate::debug_log!("SSH shell: quality {} RTT={:?} echo_rate={:?} B/s sufficient={}",
                output::route_label(&current_route), current.rtt, current.bytes_per_second, current.sufficient(options));
            if current.sufficient(options) {
                poor_samples = 0;
                continue;
            }
            poor_samples += 1;
            if poor_samples < 2 { continue; }
            poor_samples = 0;
            let mut best: Option<(Route, Arc<transport::Connection>, InteractiveQuality)> = None;
            for candidate in &routes {
                if candidate.name == current_route.name { continue; }
                match transport::connect(&config, candidate).await {
                    Ok(connection) => {
                        let connection = Arc::new(connection);
                        let quality = measure_interactive_quality(&connection).await;
                        crate::debug_log!("SSH shell: alternate quality {} RTT={:?} echo_rate={:?} B/s sufficient={}",
                            output::route_label(candidate), quality.rtt, quality.bytes_per_second, quality.sufficient(options));
                        if better_shell_route(current, quality, options) {
                            if quality.sufficient(options) {
                                let _ = switch_tx.send((candidate.clone(), connection));
                                return;
                            }
                            if best.as_ref().is_none_or(|(_, _, previous)| quality.score() < previous.score()) {
                                best = Some((candidate.clone(), connection, quality));
                            }
                        }
                    }
                    Err(error) => crate::debug_log!("SSH shell: alternate {} unavailable: {error:#}", output::route_label(candidate)),
                }
            }
            if let Some((route, connection, _)) = best {
                let _ = switch_tx.send((route, connection));
                return;
            }
        }
    }))
}

async fn measure_interactive_quality(connection: &transport::Connection) -> InteractiveQuality {
    let started = Instant::now();
    let rtt = match tokio::time::timeout(Duration::from_secs(8), connection.final_handle().send_ping()).await {
        Ok(Ok(())) => Some(started.elapsed()),
        _ => None,
    };
    let bytes_per_second = match probe_echo(connection).await {
        Ok(elapsed) => Some((INTERACTIVE_PROBE_BYTES as u128 * 1000 / elapsed.as_millis().max(1)) as u64),
        Err(error) => { crate::debug_log!("SSH shell: interactive echo probe failed: {error:#}"); None },
    };
    InteractiveQuality { rtt, bytes_per_second }
}

async fn probe_echo(connection: &transport::Connection) -> Result<Duration> {
    let work = async {
        let mut channel = connection.final_handle().channel_open_session().await?;
        channel.exec(true, "cat").await?;
        let started = Instant::now();
        channel.data_bytes(vec![b'x'; INTERACTIVE_PROBE_BYTES]).await?;
        channel.eof().await?;
        let mut received = 0;
        let mut status = None;
        while let Some(message) = channel.wait().await {
            match message {
                russh::ChannelMsg::Data { data } => received += data.len(),
                russh::ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                russh::ChannelMsg::Close => break,
                _ => {}
            }
        }
        ensure!(received == INTERACTIVE_PROBE_BYTES && status == Some(0), "interactive echo probe incomplete");
        Ok::<Duration, anyhow::Error>(started.elapsed())
    };
    tokio::time::timeout(Duration::from_secs(10), work).await.context("interactive echo probe timed out")?
}

fn choose_shell_mode(preference: ShellPreference, tmux_available: bool, screen_available: bool) -> ShellMode {
    match preference {
        ShellPreference::Auto | ShellPreference::Tmux if tmux_available => ShellMode::Tmux,
        ShellPreference::Auto | ShellPreference::Screen if screen_available => ShellMode::Screen,
        _ => ShellMode::Plain,
    }
}

struct RawTerminal;

impl RawTerminal {
    fn new() -> Result<Self> {
        crossterm::terminal::enable_raw_mode().context("cannot enable raw terminal")?;
        Ok(Self)
    }
}

impl Drop for RawTerminal {
    fn drop(&mut self) { let _ = crossterm::terminal::disable_raw_mode(); }
}

async fn remote_command_ok(connection: &transport::Connection, command: &str) -> Result<bool> {
    let work = async {
        let mut channel = connection.final_handle().channel_open_session().await?;
        channel.exec(true, command).await?;
        let mut status = None;
        while let Some(message) = channel.wait().await {
            match message {
                russh::ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                russh::ChannelMsg::Close => break,
                _ => {}
            }
        }
        Ok::<bool, anyhow::Error>(status == Some(0))
    };
    tokio::time::timeout(Duration::from_secs(10), work).await.context("remote command timed out")?
}

fn tmux_attach_command(session_name: &str) -> String {
    format!("tmux -L {session_name} -f /dev/null new-session -d -s main 2>/dev/null || true; \
        tmux -L {session_name} set-option -g status off >/dev/null 2>&1; \
        tmux -L {session_name} set-option -g prefix None >/dev/null 2>&1; \
        tmux -L {session_name} set-option -g escape-time 0 >/dev/null 2>&1; \
        exec tmux -L {session_name} attach-session -d -t main")
}

fn screen_attach_command(session_name: &str) -> String {
    format!("exec screen -c /dev/null -d -R -S {session_name}")
}

async fn shell(connection: Arc<transport::Connection>, mode: ShellMode, session_name: &str,
    stdin: &mut terminal::Input, config: Config, routes: Vec<Route>, route: Route,
    options: ShellOptions) -> Result<ShellEnd> {
    crate::debug_log!("SSH shell: opening session channel");
    let channel = connection.final_handle().channel_open_session().await?;
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let term = std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".into());
    channel
        .request_pty(true, &term, cols as u32, rows as u32, 0, 0, &[])
        .await?;
    match mode {
        ShellMode::Tmux => channel.exec(true, tmux_attach_command(session_name)).await?,
        ShellMode::Screen => channel.exec(true, screen_attach_command(session_name)).await?,
        ShellMode::Plain => channel.request_shell(true).await?,
    }
    crate::debug_log!("SSH shell: PTY requested, terminal {}x{}", cols, rows);
    let (switch_tx, mut switch_rx) = oneshot::channel();
    let monitor = if mode == ShellMode::Plain { None } else {
        monitor_shell_route(config, routes, route, connection, options, switch_tx)
    };
    let result = shell_io(channel, (cols, rows), stdin, &mut switch_rx, monitor.is_some()).await;
    if let Some(monitor) = monitor { monitor.abort(); }
    result
}

enum ShellEnd {
    InputEnded,
    ManualReconnect,
    AutoSwitch(Route, Arc<transport::Connection>),
    RemoteClosed { exited: bool },
}

async fn shell_io(
    channel: russh::Channel<russh::client::Msg>,
    mut size: (u16, u16),
    stdin: &mut terminal::Input,
    switch_rx: &mut oneshot::Receiver<ShellSwitch>,
    mut monitor_enabled: bool,
) -> Result<ShellEnd> {
    let (mut remote_read, remote_write) = channel.split();
    let mut remote_input = remote_write.make_writer();
    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut input = [0u8; 4096];
    let mut exited = false;
    let mut resize = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            switch = &mut *switch_rx, if monitor_enabled => {
                match switch {
                    Ok((route, connection)) => return Ok(ShellEnd::AutoSwitch(route, connection)),
                    Err(_) => monitor_enabled = false,
                }
            }
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
            crate::debug_log!("copy: download {}:{} -> local:{}", target, remote, local.display());
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
            crate::debug_log!("copy: upload local:{} -> {}:{}", local.display(), target, remote);
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
                crate::debug_log!("copy: uploading directory local:{} -> {}:{}", path.display(), plan.target, plan.destination);
                ensure!(plan.recursive, "source is a directory; use -r");
                directory::put_directory(config, &plan.target, &path, &plan.destination).await
            } else {
                crate::debug_log!("copy: uploading file local:{} -> {}:{}", path.display(), plan.target, plan.destination);
                transfer::put(config, &plan.target, &path, &plan.destination).await?;
                Ok(())
            }
        }
        CopySource::Remote(path) => {
            let connection = transfer::connect_any(config, &plan.target).await?;
            let sftp = transfer::sftp(&connection).await?;
            if sftp.metadata(&path).await?.file_type().is_dir() {
                crate::debug_log!("copy: downloading directory {}:{path} -> local:{}", plan.target, plan.destination);
                ensure!(plan.recursive, "source is a directory; use -r");
                directory::get_directory(config, &plan.target, &path, Path::new(&plan.destination)).await
            } else {
                crate::debug_log!("copy: downloading file {}:{path} -> local:{}", plan.target, plan.destination);
                transfer::get(config, &plan.target, &path, Path::new(&plan.destination)).await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod shell_tests {
    use super::*;

    fn options() -> ShellOptions {
        ShellOptions {
            preference: ShellPreference::Auto,
            min_rate_bytes_per_sec: 2048,
            max_rtt: Duration::from_millis(2000),
            check_interval: Some(Duration::from_secs(30)),
        }
    }

    #[test]
    fn chooses_installed_terminal_multiplexer_in_order() {
        assert_eq!(choose_shell_mode(ShellPreference::Auto, true, true), ShellMode::Tmux);
        assert_eq!(choose_shell_mode(ShellPreference::Auto, false, true), ShellMode::Screen);
        assert_eq!(choose_shell_mode(ShellPreference::Auto, false, false), ShellMode::Plain);
        assert_eq!(choose_shell_mode(ShellPreference::Screen, true, true), ShellMode::Screen);
        assert_eq!(choose_shell_mode(ShellPreference::Tmux, false, true), ShellMode::Plain);
        assert_eq!(choose_shell_mode(ShellPreference::Plain, true, true), ShellMode::Plain);
    }

    #[test]
    fn interactive_quality_prefers_usable_route_and_keeps_slow_fallback() {
        let slow = InteractiveQuality {
            rtt: Some(Duration::from_millis(3000)),
            bytes_per_second: Some(500),
        };
        let usable = InteractiveQuality {
            rtt: Some(Duration::from_millis(500)),
            bytes_per_second: Some(4000),
        };
        let slightly_better_but_slow = InteractiveQuality {
            rtt: Some(Duration::from_millis(2800)),
            bytes_per_second: Some(600),
        };
        assert!(!slow.sufficient(options()));
        assert!(usable.sufficient(options()));
        assert!(better_shell_route(slow, usable, options()));
        assert!(!better_shell_route(slow, slightly_better_but_slow, options()));
        assert!(better_shell_route(slow, InteractiveQuality {
            rtt: Some(Duration::from_millis(1200)),
            bytes_per_second: Some(1500),
        }, options()));
    }
}
