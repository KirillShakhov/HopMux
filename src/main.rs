mod config;
mod debug;
mod directory;
mod service;
mod terminal;
mod transfer;
mod transport;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use config::Config;
use service::PartialAction;
use std::{io::{self, Write}, path::PathBuf, process::{Command, Stdio}};

#[derive(Parser)]
#[command(name = "mssh", version, about = "Multi-hop SSH and file copy")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long = "route")]
    routes: Vec<String>,
    #[arg(long)]
    max_routes: Option<usize>,
    #[arg(long)]
    chunk_size: Option<usize>,
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(short = 'd', long)]
    background: bool,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    details: bool,
    #[arg(long)]
    debug: bool,
    #[arg(long, conflicts_with = "restart")]
    continue_partial: bool,
    #[arg(long, conflicts_with = "continue_partial")]
    restart: bool,
    #[arg(long, hide = true)]
    worker_action: Option<String>,
    #[arg(required = true, num_args = 1..)]
    args: Vec<String>,
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
    debug::init(cli.debug);
    debug_log!("command: {:?}", cli.args);
    let path = cli.config.clone().unwrap_or(config::default_path()?);
    debug_log!("loading configuration from {}", path.display());
    let mut config = Config::load(&path)?;
    debug_log!("configuration loaded: {} nodes, {} routes", config.nodes.len(), config.effective_routes().len());
    for name in &cli.routes {
        ensure!(config.effective_routes().iter().any(|r| &r.name == name && !r.disabled), "unknown or disabled route: {name}");
    }
    config.route_filter = cli.routes.clone();
    debug_log!("selected routes: {}", if cli.routes.is_empty() { "all".to_owned() } else { cli.routes.join(", ") });
    if let Some(chunk_size) = cli.chunk_size {
        ensure!((64 * 1024..=8 * 1024 * 1024).contains(&chunk_size), "chunk size must be 64 KiB..8 MiB");
        config.settings.chunk_size = chunk_size;
    }
    if let Some(max_routes) = cli.max_routes {
        ensure!(max_routes > 0, "max-routes must be positive");
        config.settings.max_parallel_routes = max_routes;
    } else {
        config.settings.max_parallel_routes = config.effective_routes().len().max(1);
    }
    debug_log!("transfer settings: max_routes={}, chunk_size={} bytes", config.settings.max_parallel_routes, config.settings.chunk_size);
    let unpinned: Vec<_> = config.nodes.iter().filter_map(|(id, node)| node.host_key.is_none().then_some(id.as_str())).collect();
    if !unpinned.is_empty() {
        eprintln!("warning: SSH host keys are not verified for nodes: {}", unpinned.join(", "));
    }
    match cli.args.as_slice() {
        [cmd, check] if cmd == "config" && check == "check" => {
            debug_log!("validating configuration");
            println!("config OK: {} nodes, {} routes", config.nodes.len(), config.effective_routes().len());
        }
        [cmd, target] if cmd == "transfers" => {
            debug_log!("listing transfers for {target}");
            show_transfers(&config, target, cli.json).await?
        },
        [cmd] if cmd == "status" => show_status(&config, None, cli.json, cli.details).await?,
        [cmd, target] if cmd == "status" => show_status(&config, Some(target), cli.json, cli.details).await?,
        [target] if config.nodes.contains_key(target) => {
            debug_log!("opening SSH shell to {target}");
            service::ssh(&config, target).await?
        },
        [source, destination] => {
            debug_log!("preparing copy: {source} -> {destination}");
            let plan = service::prepare_copy(&config, source, destination, cli.recursive).await?;
            let action = choose_action(&cli, &plan)?;
            if cli.background {
                debug_log!("starting background copy worker");
                spawn_worker(&cli, &path, action)?;
                println!("transfer started in background");
            } else {
                debug_log!("running copy in foreground");
                service::execute_copy(&config, plan, action).await?;
                println!("transfer complete");
            }
        }
        _ => bail!("usage: mssh TARGET | mssh SOURCE DESTINATION | mssh status [TARGET] | mssh transfers TARGET | mssh config check"),
    }
    Ok(())
}

fn choose_action(cli: &Cli, plan: &service::CopyPlan) -> Result<PartialAction> {
    if plan.partial.is_none() { debug_log!("no partial transfer found"); return Ok(PartialAction::New); }
    debug_log!("partial transfer found: {}", plan.partial.as_ref().unwrap().id);
    if cli.continue_partial || cli.worker_action.as_deref() == Some("continue") { debug_log!("continuing partial transfer"); return Ok(PartialAction::Continue); }
    if cli.restart || cli.worker_action.as_deref() == Some("restart") { debug_log!("restarting partial transfer"); return Ok(PartialAction::Restart); }
    ensure!(cli.worker_action.is_none(), "partial transfer appeared after background worker started; choose --continue-partial or --restart");
    ensure!(!cli.background || atty_stdin(), "partial transfer exists; use --continue-partial or --restart with -d");
    eprint!("Partial transfer found. [c]ontinue or [r]estart? ");
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "c" | "continue" => { debug_log!("user selected continue"); Ok(PartialAction::Continue) },
        "r" | "restart" => { debug_log!("user selected restart"); Ok(PartialAction::Restart) },
        _ => bail!("transfer left unchanged"),
    }
}

fn atty_stdin() -> bool {
    std::io::IsTerminal::is_terminal(&io::stdin())
}

fn spawn_worker(cli: &Cli, config_path: &std::path::Path, action: PartialAction) -> Result<()> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--config").arg(config_path);
    for route in &cli.routes { cmd.arg("--route").arg(route); }
    if let Some(n) = cli.max_routes { cmd.arg("--max-routes").arg(n.to_string()); }
    if let Some(n) = cli.chunk_size { cmd.arg("--chunk-size").arg(n.to_string()); }
    if cli.recursive { cmd.arg("-r"); }
    if cli.debug { cmd.arg("--debug"); }
    cmd.arg("--worker-action").arg(match action {
        PartialAction::Continue => "continue", PartialAction::Restart => "restart", PartialAction::New => "new",
    });
    cmd.args(&cli.args).stdin(Stdio::null()).stdout(Stdio::null())
        .stderr(if cli.debug { Stdio::inherit() } else { Stdio::null() });
    #[cfg(unix)] {
        use std::os::unix::process::CommandExt;
        unsafe { cmd.pre_exec(|| { if libc::setsid() < 0 { return Err(io::Error::last_os_error()); } Ok(()) }); }
    }
    #[cfg(windows)] {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x00000008 | 0x00000200);
    }
    let child = cmd.spawn().context("cannot start background transfer")?;
    debug_log!("background worker spawned with pid {}", child.id());
    println!("worker pid: {}", child.id());
    Ok(())
}

async fn show_transfers(config: &Config, target: &str, json: bool) -> Result<()> {
    ensure!(config.nodes.contains_key(target), "unknown target: {target}");
    let journals = transfer::list(config, target).await?;
    debug_log!("found {} transfer journals on {target}", journals.len());
    if json { println!("{}", serde_json::to_string_pretty(&journals)?); }
    else {
        for j in journals {
            let done = j.complete.iter().filter(|&&x| x).count();
            println!("{} {:?} {} -> {} {}/{} chunks{}", j.id, j.direction, j.source, j.destination, done, j.complete.len(), if j.published { " [done]" } else { " [partial]" });
            if let Some(error) = &j.last_error { println!("  error: {error}"); }
            for (route, stats) in j.route_stats { println!("  {route}: {} bytes, errors {}, reconnects {}", stats.bytes, stats.errors, stats.reconnects); }
        }
    }
    Ok(())
}

async fn show_status(config: &Config, target: Option<&str>, json: bool, details: bool) -> Result<()> {
    if let Some(target) = target { ensure!(config.nodes.contains_key(target), "unknown target: {target}"); }
    let routes = service::probe_routes(config, target).await;
    debug_log!("probed {} routes", routes.len());
    let all_down = routes.iter().all(|r| r.state != "UP");
    let configured_routes: Vec<_> = config.effective_routes().into_iter()
        .filter(|route| target.is_none_or(|t| route.target == t)).collect();
    let auth = service::auth_statuses(config);
    let mut transfers = std::collections::BTreeMap::new();
    for node in config.nodes.keys().filter(|node| target.is_none_or(|t| node.as_str() == t)) {
        if routes.iter().any(|r| &r.target == node && r.state == "UP") {
            transfers.insert(node.clone(), transfer::list(config, node).await?);
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&serde_json::json!({"configured_routes": configured_routes, "routes": routes, "auth": auth, "transfers": transfers}))?);
    } else {
        if details {
            for node in &auth {
                println!("auth {}: key={} password={} agent={} usable={}", node.node, node.key_available, node.password_available, node.agent_available, node.usable);
            }
            for route in &configured_routes {
                if route.disabled { println!("{} -> {} [disabled]", route.name, route.target); }
            }
        }
        for route in &routes {
            println!("{} -> {}: {} connect={:?}ms rtt={:?}ms{}", route.route, route.target, route.state, route.connect_ms, route.rtt_ms, route.error.as_ref().map_or(String::new(), |e| format!(" {e}")));
            if details {
                println!("  via {}", route.hops.join(" -> "));
                for hop in &route.hop_timings { println!("  {hop:?}"); }
            }
        }
        for (node, journals) in transfers {
            for j in journals {
                let done = j.complete.iter().filter(|&&x| x).count();
                println!("transfer {} on {}: {:?} {}/{} chunks{}", j.id, node, j.direction, done, j.complete.len(), if j.published { " [done]" } else { " [partial]" });
                if let Some(error) = &j.last_error { println!("  error: {error}"); }
                if details {
                    for (route, stats) in j.route_stats { println!("  {route}: {} bytes, errors {}, reconnects {}", stats.bytes, stats.errors, stats.reconnects); }
                }
            }
        }
    }
    if all_down { bail!("no available routes"); }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scp_style_copy_and_background_options() {
        let copy = Cli::try_parse_from(["mssh", "edge_a:/home/test.yml", "./downloads/"]).unwrap();
        assert_eq!(copy.args, ["edge_a:/home/test.yml", "./downloads/"]);
        let upload = Cli::try_parse_from([
            "mssh", "-d", "--route", "via-a", "--continue-partial",
            "./test.yml", "edge_a:/home",
        ]).unwrap();
        assert!(upload.background);
        assert!(upload.continue_partial);
        assert_eq!(upload.routes, ["via-a"]);
        let debug_status = Cli::try_parse_from(["mssh", "--debug", "status", "edge_a"]).unwrap();
        assert!(debug_status.debug);
        assert!(!debug_status.details);
        let detailed_status = Cli::try_parse_from(["mssh", "status", "edge_a", "--details"]).unwrap();
        assert!(detailed_status.details);
        assert!(!detailed_status.debug);
    }
}
