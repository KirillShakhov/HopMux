mod config;
mod directory;
mod terminal;
mod transfer;
mod transport;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use config::{Config, Route};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, sync::Semaphore};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "mssh", version, about = "Multi-hop SSH client")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Routes {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Status {
        #[arg(long)]
        watch: bool,
        #[arg(long)]
        routes: bool,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        details: bool,
        #[arg(long)]
        cached: bool,
        #[arg(long)]
        json: bool,
    },
    Doctor {
        target: String,
        #[arg(long)]
        json: bool,
    },
    Ssh {
        target: String,
    },
    Auth {
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
    Put {
        #[arg(short, long)]
        recursive: bool,
        target: String,
        source: PathBuf,
        destination: String,
    },
    Get {
        #[arg(short, long)]
        recursive: bool,
        target: String,
        source: String,
        destination: PathBuf,
    },
    Transfer {
        #[command(subcommand)]
        command: TransferCommand,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    Check,
}

#[derive(Subcommand)]
enum TransferCommand {
    List {
        #[arg(long)]
        json: bool,
    },
    Resume {
        id: Uuid,
    },
    Cancel {
        id: Uuid,
    },
}

#[derive(Clone, Deserialize, Serialize)]
struct RouteStatus {
    route: String,
    target: String,
    state: String,
    connect_ms: Option<u128>,
    #[serde(default)]
    rtt_ms: Option<u128>,
    #[serde(default)]
    checked_at_unix_ms: u128,
    error: Option<String>,
    hops: Vec<String>,
    #[serde(default)]
    hop_timings: Vec<transport::HopTiming>,
    #[serde(default)]
    last_success_unix_ms: Option<u128>,
    #[serde(default)]
    probe_errors: u64,
    #[serde(default)]
    probe_reconnects: u64,
    #[serde(default)]
    error_count: u64,
    #[serde(default)]
    reconnect_count: u64,
    #[serde(default)]
    transferred_bytes: u64,
    #[serde(default)]
    last_bps: Option<f64>,
    #[serde(default)]
    current_bps: Option<f64>,
}

#[derive(Serialize)]
struct EntryStatus {
    entry: String,
    state: String,
    best_route: String,
    route_count: usize,
    up_routes: usize,
    connect_ms: Option<u128>,
    rtt_ms: Option<u128>,
    last_success_unix_ms: Option<u128>,
    transferred_bytes: u64,
    last_bps: Option<f64>,
    current_bps: Option<f64>,
    error_count: u64,
    reconnect_count: u64,
}

#[derive(Serialize)]
struct AuthStatus {
    node: String,
    key_file: Option<String>,
    key_available: bool,
    password_env: Option<String>,
    password_available: bool,
    agent: bool,
    agent_available: bool,
    usable: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("mssh: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let path = cli.config.unwrap_or(config::default_path()?);
    let config = Config::load(&path)?;
    match cli.command {
        Command::Config {
            command: ConfigCommand::Check,
        } => println!(
            "config OK: {} nodes, {} routes",
            config.nodes.len(),
            config.routes.len()
        ),
        Command::Routes { target, json } => {
            let routes: Vec<_> = config
                .routes
                .iter()
                .filter(|r| target.as_ref().is_none_or(|t| &r.target == t))
                .collect();
            if json {
                println!("{}", serde_json::to_string_pretty(&routes)?);
            } else {
                for route in routes {
                    println!(
                        "{} -> {} via {}{}",
                        route.name,
                        route.target,
                        route.hops.join(" -> "),
                        if route.disabled { " [disabled]" } else { "" }
                    );
                }
            }
        }
        Command::Status {
            watch,
            routes,
            target,
            details,
            cached,
            json,
        } => loop {
            let statuses = if cached {
                load_status()?
            } else {
                let mut statuses = probe_routes(&config, target.as_deref()).await;
                enrich_statuses(&mut statuses)?;
                save_status(&statuses)?;
                statuses
            };
            let statuses: Vec<_> = statuses
                .into_iter()
                .filter(|s| target.as_ref().is_none_or(|t| &s.target == t))
                .collect();
            if routes || details || target.is_some() {
                print_route_statuses(&statuses, json, details)?;
            } else {
                print_entry_statuses(&statuses, json)?;
            }
            if !watch || cached {
                if statuses.iter().all(|s| s.state != "UP") {
                    bail!("no available routes");
                }
                break;
            }
            tokio::time::sleep(Duration::from_secs(config.settings.check_interval_secs)).await;
        },
        Command::Doctor { target, json } => {
            let mut statuses = probe_routes(&config, Some(&target)).await;
            enrich_statuses(&mut statuses)?;
            save_status(&statuses)?;
            if statuses.is_empty() {
                bail!("no route to target {target}");
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&statuses)?);
            } else {
                for s in &statuses {
                    println!(
                        "{}: {} {}",
                        s.route,
                        s.state,
                        s.error.as_deref().unwrap_or("")
                    );
                }
            }
            if statuses.iter().all(|s| s.state != "UP") {
                bail!("no available route to {target}");
            }
        }
        Command::Ssh { target } => ssh(&config, &target).await?,
        Command::Auth { check, json } => {
            let statuses = auth_statuses(&config);
            if json {
                println!("{}", serde_json::to_string_pretty(&statuses)?);
            } else {
                for s in &statuses {
                    println!(
                        "{}: key={} password={} agent={} usable={}",
                        s.node, s.key_available, s.password_available, s.agent_available, s.usable
                    );
                }
            }
            if check && statuses.iter().any(|s| !s.usable) {
                bail!("one or more nodes have no available authentication method");
            }
        }
        Command::Put {
            recursive,
            target,
            source,
            destination,
        } => {
            if recursive {
                directory::put_directory(&config, &target, &source, &destination).await?;
                println!("directory uploaded");
            } else {
                let id = transfer::put(&config, &target, &source, &destination).await?;
                println!("uploaded: {id}");
            }
        }
        Command::Get {
            recursive,
            target,
            source,
            destination,
        } => {
            if recursive {
                directory::get_directory(&config, &target, &source, &destination).await?;
                println!("directory downloaded");
            } else {
                let id = transfer::get(&config, &target, &source, &destination).await?;
                println!("downloaded: {id}");
            }
        }
        Command::Transfer { command } => match command {
            TransferCommand::List { json } => {
                let journals = transfer::list()?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&journals)?);
                } else {
                    for j in journals {
                        let completed = j.complete.iter().filter(|&&done| done).count();
                        println!(
                            "{} {:?} {} -> {} {}/{} chunks ({:.1}%){}",
                            j.id,
                            j.direction,
                            j.source,
                            j.destination,
                            completed,
                            j.complete.len(),
                            if j.complete.is_empty() {
                                100.0
                            } else {
                                100.0 * completed as f64 / j.complete.len() as f64
                            },
                            if j.published {
                                " [published]"
                            } else if transfer::is_active(&j) {
                                " [active]"
                            } else {
                                " [interrupted]"
                            }
                        );
                        for (route, stats) in &j.route_stats {
                            println!(
                                "  {route}: {} bytes, last {}, errors {}, reconnects {}",
                                stats.bytes,
                                format_rate(stats.last_bps),
                                stats.errors,
                                stats.reconnects
                            );
                        }
                    }
                }
            }
            TransferCommand::Resume { id } => transfer::resume(&config, id).await?,
            TransferCommand::Cancel { id } => {
                transfer::cancel(&config, id).await?;
                println!("cancelled {id}");
            }
        },
    }
    Ok(())
}

fn auth_statuses(config: &Config) -> Vec<AuthStatus> {
    config
        .nodes
        .iter()
        .map(|(id, node)| {
            let key_available = node.key_file.as_ref().is_some_and(|path| {
                let expanded = if let Some(rest) = path.strip_prefix("~/") {
                    dirs::home_dir().map(|home| home.join(rest))
                } else {
                    Some(PathBuf::from(path))
                };
                expanded.is_some_and(|path| path.is_file())
            });
            let password_available = node
                .password_env
                .as_ref()
                .is_some_and(|name| std::env::var_os(name).is_some());
            let agent_available = node.agent
                && cfg!(unix)
                && std::env::var_os("SSH_AUTH_SOCK")
                    .is_some_and(|path| std::path::Path::new(&path).exists());
            AuthStatus {
                node: id.clone(),
                key_file: node.key_file.clone(),
                key_available,
                password_env: node.password_env.clone(),
                password_available,
                agent: node.agent,
                agent_available,
                usable: key_available || password_available || agent_available,
            }
        })
        .collect()
}

fn enrich_statuses(statuses: &mut [RouteStatus]) -> Result<()> {
    let previous: BTreeMap<_, _> = load_status()
        .unwrap_or_default()
        .into_iter()
        .map(|status| (status.route.clone(), status))
        .collect();
    let journals = transfer::list()?;
    for status in statuses {
        if let Some(old) = previous.get(&status.route) {
            status.probe_errors = old.probe_errors;
            status.probe_reconnects = old.probe_reconnects;
            status.last_success_unix_ms = old.last_success_unix_ms;
            if status.state == "UP" && old.state != "UP" {
                status.probe_reconnects = status.probe_reconnects.saturating_add(1);
            }
        }
        if status.state == "UP" {
            status.last_success_unix_ms = Some(status.checked_at_unix_ms);
        } else {
            status.probe_errors = status.probe_errors.saturating_add(1);
        }
        status.error_count = status.probe_errors;
        status.reconnect_count = status.probe_reconnects;
        let mut last_measurement = 0;
        for journal in &journals {
            if journal.target != status.target {
                continue;
            }
            let Some(stats) = journal.route_stats.get(&status.route) else {
                continue;
            };
            status.transferred_bytes = status.transferred_bytes.saturating_add(stats.bytes);
            status.error_count = status.error_count.saturating_add(stats.errors);
            status.reconnect_count = status.reconnect_count.saturating_add(stats.reconnects);
            if stats.last_at_unix_ms > last_measurement {
                last_measurement = stats.last_at_unix_ms;
                status.last_bps = stats.last_bps;
            }
            if transfer::is_active(journal)
                && status
                    .checked_at_unix_ms
                    .saturating_sub(stats.last_at_unix_ms)
                    <= 5_000
            {
                status.current_bps =
                    Some(status.current_bps.unwrap_or(0.0) + stats.last_bps.unwrap_or(0.0));
            }
        }
    }
    Ok(())
}

fn entry_statuses(routes: &[RouteStatus]) -> Vec<EntryStatus> {
    let mut groups: BTreeMap<String, Vec<&RouteStatus>> = BTreeMap::new();
    for route in routes {
        if let Some(entry) = route.hops.first() {
            groups.entry(entry.clone()).or_default().push(route);
        }
    }
    groups
        .into_iter()
        .filter_map(|(entry, group)| {
            let best = group
                .iter()
                .copied()
                .filter(|s| s.state == "UP")
                .min_by_key(|s| {
                    (
                        s.rtt_ms.unwrap_or(u128::MAX),
                        s.connect_ms.unwrap_or(u128::MAX),
                    )
                })
                .or_else(|| group.first().copied())?;
            let sum_rates = |values: Vec<Option<f64>>| {
                let present: Vec<f64> = values.into_iter().flatten().collect();
                (!present.is_empty()).then_some(present.iter().sum())
            };
            Some(EntryStatus {
                entry,
                state: best.state.clone(),
                best_route: best.route.clone(),
                route_count: group.len(),
                up_routes: group.iter().filter(|s| s.state == "UP").count(),
                connect_ms: best.connect_ms,
                rtt_ms: best.rtt_ms,
                last_success_unix_ms: group.iter().filter_map(|s| s.last_success_unix_ms).max(),
                transferred_bytes: group
                    .iter()
                    .fold(0u64, |total, s| total.saturating_add(s.transferred_bytes)),
                last_bps: sum_rates(group.iter().map(|s| s.last_bps).collect()),
                current_bps: sum_rates(group.iter().map(|s| s.current_bps).collect()),
                error_count: group
                    .iter()
                    .fold(0u64, |total, s| total.saturating_add(s.error_count)),
                reconnect_count: group
                    .iter()
                    .fold(0u64, |total, s| total.saturating_add(s.reconnect_count)),
            })
        })
        .collect()
}

fn format_rate(rate: Option<f64>) -> String {
    rate.map_or("-".into(), |bps| format!("{:.1} KiB/s", bps / 1024.0))
}

fn checked_age(checked_at_unix_ms: u128) -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .saturating_sub(checked_at_unix_ms)
        / 1000
}

fn print_route_statuses(statuses: &[RouteStatus], json: bool, details: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(statuses)?);
    } else {
        for s in statuses {
            println!(
                "{}: {} | connect {} ms, RTT {} ms | last {} | {} bytes | errors {}, reconnects {}{}",
                s.route,
                s.state,
                s.connect_ms.map_or("-".into(), |n| n.to_string()),
                s.rtt_ms.map_or("-".into(), |n| n.to_string()),
                format_rate(s.last_bps),
                s.transferred_bytes,
                s.error_count,
                s.reconnect_count,
                s.error.as_ref().map_or("".into(), |e| format!(" — {e}"))
            );
            if details {
                for hop in &s.hop_timings {
                    println!("  {}: {} ms", hop.node, hop.connect_ms);
                }
                if s.hop_timings.is_empty() {
                    println!("  {}", s.hops.join(" -> "));
                }
                if let Some(last) = s.last_success_unix_ms {
                    println!("  last success: {} s ago", checked_age(last));
                }
                if let Some(current) = s.current_bps {
                    println!("  current speed: {}", format_rate(Some(current)));
                }
            }
        }
    }
    Ok(())
}

fn print_entry_statuses(statuses: &[RouteStatus], json: bool) -> Result<()> {
    let entries = entry_statuses(statuses);
    if json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else {
        for entry in entries {
            println!(
                "{}: {} ({}/{} routes, best {}) | RTT {} ms | last {} | {} bytes | errors {}, reconnects {}",
                entry.entry,
                entry.state,
                entry.up_routes,
                entry.route_count,
                entry.best_route,
                entry.rtt_ms.map_or("-".into(), |n| n.to_string()),
                format_rate(entry.last_bps),
                entry.transferred_bytes,
                entry.error_count,
                entry.reconnect_count
            );
        }
    }
    Ok(())
}

fn status_path() -> Result<PathBuf> {
    Ok(dirs::state_dir()
        .or_else(|| dirs::home_dir().map(|p| p.join(".local/state")))
        .context("cannot find state directory")?
        .join("mssh/status.json"))
}

fn save_status(statuses: &[RouteStatus]) -> Result<()> {
    let path = status_path()?;
    std::fs::create_dir_all(path.parent().unwrap())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )?;
    }
    let temporary = path.with_extension(format!("json.tmp-{}", Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(&temporary)?;
    let mut merged: BTreeMap<_, _> = load_status()
        .unwrap_or_default()
        .into_iter()
        .map(|status| (status.route.clone(), status))
        .collect();
    for status in statuses {
        merged.insert(status.route.clone(), status.clone());
    }
    file.write_all(&serde_json::to_vec_pretty(
        &merged.into_values().collect::<Vec<_>>(),
    )?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn load_status() -> Result<Vec<RouteStatus>> {
    Ok(serde_json::from_slice(
        &std::fs::read(status_path()?).context("no cached status available")?,
    )?)
}

async fn probe_routes(config: &Config, target: Option<&str>) -> Vec<RouteStatus> {
    let routes: Vec<Route> = config
        .routes
        .iter()
        .filter(|r| !r.disabled && target.is_none_or(|t| r.target == t))
        .cloned()
        .collect();
    let shared = Arc::new(config.clone());
    let semaphore = Arc::new(Semaphore::new(config.settings.max_parallel_routes));
    let mut tasks = Vec::new();
    for route in routes {
        let shared = shared.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            let result = transport::connect(&shared, &route).await;
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
                        last_success_unix_ms: None,
                        probe_errors: 0,
                        probe_reconnects: 0,
                        error_count: 0,
                        reconnect_count: 0,
                        transferred_bytes: 0,
                        last_bps: None,
                        current_bps: None,
                    }
                }
                Err(error) => RouteStatus {
                    route: route.name,
                    target: route.target,
                    state: transport::failure_state(&error).into(),
                    connect_ms: None,
                    rtt_ms: None,
                    checked_at_unix_ms,
                    error: Some(format!("{error:#}")),
                    hops: route.hops,
                    hop_timings: Vec::new(),
                    last_success_unix_ms: None,
                    probe_errors: 0,
                    probe_reconnects: 0,
                    error_count: 0,
                    reconnect_count: 0,
                    transferred_bytes: 0,
                    last_bps: None,
                    current_bps: None,
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

async fn ssh(config: &Config, target: &str) -> Result<()> {
    let mut routes = config.routes_for(target);
    if routes.is_empty() {
        bail!("no route to target {target}");
    }
    let mut index = 0;
    loop {
        let route = routes[index % routes.len()];
        index += 1;
        match transport::connect(config, route).await {
            Ok(connection) => {
                eprintln!("connected via {} (Ctrl-] to reconnect)", route.name);
                match shell(&connection).await {
                    Ok(true) => return Ok(()),
                    Ok(false) => eprintln!("reconnecting..."),
                    Err(error) => eprintln!("shell disconnected: {error:#}; reconnecting..."),
                }
            }
            Err(error) => {
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
    let channel = connection.final_handle().channel_open_session().await?;
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let term = std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".into());
    channel
        .request_pty(true, &term, cols as u32, rows as u32, 0, 0, &[])
        .await?;
    channel.request_shell(true).await?;
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
