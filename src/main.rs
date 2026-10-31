mod config;
mod directory;
mod terminal;
mod transfer;
mod transport;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use config::{Config, Route};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
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

#[derive(Deserialize, Serialize)]
struct RouteStatus {
    route: String,
    target: String,
    state: String,
    connect_ms: Option<u128>,
    error: Option<String>,
    hops: Vec<String>,
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
            routes: _,
            target,
            details,
            cached,
            json,
        } => loop {
            let statuses = if cached {
                load_status()?
            } else {
                let statuses = probe_routes(&config, target.as_deref()).await;
                save_status(&statuses)?;
                statuses
            };
            let statuses: Vec<_> = statuses
                .into_iter()
                .filter(|s| target.as_ref().is_none_or(|t| &s.target == t))
                .collect();
            if json {
                println!("{}", serde_json::to_string_pretty(&statuses)?);
            } else {
                for s in &statuses {
                    println!(
                        "{}: {} ({} ms){}",
                        s.route,
                        s.state,
                        s.connect_ms.map_or("-".into(), |n| n.to_string()),
                        s.error.as_ref().map_or("".into(), |e| format!(" — {e}"))
                    );
                    if details {
                        println!("  {}", s.hops.join(" -> "));
                    }
                }
            }
            if !watch || cached {
                break;
            }
            tokio::time::sleep(Duration::from_secs(config.settings.check_interval_secs)).await;
        },
        Command::Doctor { target, json } => {
            let statuses = probe_routes(&config, Some(&target)).await;
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
        }
        Command::Ssh { target } => ssh(&config, &target).await?,
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
                        println!(
                            "{} {:?} {} -> {} {}/{}",
                            j.id,
                            j.direction,
                            j.source,
                            j.destination,
                            j.complete.iter().filter(|&&done| done).count(),
                            j.complete.len()
                        );
                    }
                }
            }
            TransferCommand::Resume { id } => transfer::resume(&config, id).await?,
            TransferCommand::Cancel { id } => {
                let temporary = transfer::cancel(id)?;
                println!("cancelled {id}; temporary file retained at {temporary}");
            }
        },
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
    file.write_all(&serde_json::to_vec_pretty(statuses)?)?;
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
            match result {
                Ok(conn) => RouteStatus {
                    route: route.name,
                    target: route.target,
                    state: "UP".into(),
                    connect_ms: Some(conn.elapsed.as_millis()),
                    error: None,
                    hops: route.hops,
                },
                Err(error) => RouteStatus {
                    route: route.name,
                    target: route.target,
                    state: transport::failure_state(&error).into(),
                    connect_ms: None,
                    error: Some(format!("{error:#}")),
                    hops: route.hops,
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
