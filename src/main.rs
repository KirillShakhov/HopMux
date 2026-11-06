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
    chunk_size: Option<String>,
    #[arg(long)]
    min_chunk_size: Option<String>,
    #[arg(long)]
    max_chunk_size: Option<String>,
    #[arg(long = "route-chunk")]
    route_chunks: Vec<String>,
    #[arg(long)]
    chunk_timeout: Option<u64>,
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
    #[arg(long)]
    verify: bool,
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
    config.verify = cli.verify;
    debug_log!("selected routes: {}", if cli.routes.is_empty() { "all".to_owned() } else { cli.routes.join(", ") });
    if let Some(chunk_size) = &cli.chunk_size {
        if chunk_size.eq_ignore_ascii_case("auto") {
            config.auto_chunk = true;
        } else {
            config.settings.chunk_size = parse_size(chunk_size)?;
            config.auto_chunk = false;
        }
    }
    if let Some(size) = &cli.min_chunk_size { config.min_chunk_size = parse_size(size)?; }
    if let Some(size) = &cli.max_chunk_size { config.max_chunk_size = parse_size(size)?; }
    ensure!(config.min_chunk_size >= 64 * 1024 && config.max_chunk_size <= 32 * 1024 * 1024
        && config.min_chunk_size <= config.max_chunk_size, "chunk limits must be 64 KiB..32 MiB with min <= max");
    config.chunk_timeout_secs = cli.chunk_timeout;
    ensure!(config.chunk_timeout_secs.is_none_or(|secs| (1..=300).contains(&secs)), "chunk timeout must be 1..=300 seconds");
    for override_spec in &cli.route_chunks {
        let (route, size) = override_spec.split_once('=').context("route-chunk must be ROUTE=SIZE")?;
        ensure!(config.effective_routes().iter().any(|r| r.name == route && !r.disabled), "unknown or disabled route: {route}");
        let size = parse_size(size)?;
        ensure!((64 * 1024..=32 * 1024 * 1024).contains(&size), "route chunk size must be 64 KiB..32 MiB");
        config.route_chunk_sizes.insert(route.to_owned(), size);
    }
    if let Some(max_routes) = cli.max_routes {
        ensure!((1..=16).contains(&max_routes), "max-routes must be 1..=16");
        config.settings.max_parallel_routes = max_routes;
    } else {
        config.settings.max_parallel_routes = config.effective_routes().len().max(1);
    }
    config.settings.max_parallel_routes = config.settings.max_parallel_routes.min(16);
    let largest_batch = if config.auto_chunk { config.max_chunk_size } else { config.settings.chunk_size }
        .max(config.route_chunk_sizes.values().copied().max().unwrap_or(0));
    ensure!(largest_batch.saturating_mul(config.settings.max_parallel_routes).saturating_mul(2) <= 256 * 1024 * 1024,
        "chunk sizes and parallel routes exceed the 256 MiB transfer buffer budget");
    debug_log!("transfer settings: max_routes={}, chunk_mode={}, initial_chunk={} bytes, min={}, max={}", config.settings.max_parallel_routes, if config.auto_chunk { "auto" } else { "fixed" }, config.settings.chunk_size, config.min_chunk_size, config.max_chunk_size);
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

fn parse_size(input: &str) -> Result<usize> {
    let lower = input.to_ascii_lowercase();
    let (number, scale) = if let Some(value) = lower.strip_suffix("mib") { (value, 1024 * 1024) }
        else if let Some(value) = lower.strip_suffix("kib") { (value, 1024) }
        else if let Some(value) = lower.strip_suffix("mb") { (value, 1000 * 1000) }
        else if let Some(value) = lower.strip_suffix("kb") { (value, 1000) }
        else { (lower.as_str(), 1) };
    let value = number.parse::<usize>().with_context(|| format!("invalid size: {input}"))?;
    let bytes = value.checked_mul(scale).context("size is too large")?;
    ensure!((64 * 1024..=32 * 1024 * 1024).contains(&bytes), "size must be 64 KiB..32 MiB");
    ensure!(bytes.is_multiple_of(64 * 1024), "size must be a multiple of 64 KiB");
    Ok(bytes)
}

fn spawn_worker(cli: &Cli, config_path: &std::path::Path, action: PartialAction) -> Result<()> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--config").arg(config_path);
    for route in &cli.routes { cmd.arg("--route").arg(route); }
    if let Some(n) = cli.max_routes { cmd.arg("--max-routes").arg(n.to_string()); }
    if let Some(n) = &cli.chunk_size { cmd.arg("--chunk-size").arg(n); }
    if let Some(n) = &cli.min_chunk_size { cmd.arg("--min-chunk-size").arg(n); }
    if let Some(n) = &cli.max_chunk_size { cmd.arg("--max-chunk-size").arg(n); }
    for route_chunk in &cli.route_chunks { cmd.arg("--route-chunk").arg(route_chunk); }
    if let Some(seconds) = cli.chunk_timeout { cmd.arg("--chunk-timeout").arg(seconds.to_string()); }
    if cli.recursive { cmd.arg("-r"); }
    if cli.debug { cmd.arg("--debug"); }
    if cli.verify { cmd.arg("--verify"); }
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
            println!("{} {:?} {} -> {} {}/{} chunks{}{}", j.id, j.direction, j.source, j.destination, done, j.complete.len(), if j.published { " [done]" } else { " [partial]" }, if j.verify { " [SHA-256]" } else { "" });
            if let Some(error) = &j.last_error { println!("  error: {error}"); }
            for (route, stats) in j.route_stats { println!("  {route}: {} bytes, batch {} bytes, connect {} ms, session {} ms, failed {} ms, errors {}, reconnects {}", stats.bytes, if stats.chosen_batch_size > 0 { stats.chosen_batch_size } else { j.batch_size.max(j.chunk_size) }, stats.connect_ms, stats.session_ms, stats.failed_session_ms, stats.errors, stats.reconnects); }
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
                println!("transfer {} on {}: {:?} {}/{} chunks{}{}", j.id, node, j.direction, done, j.complete.len(), if j.published { " [done]" } else { " [partial]" }, if j.verify { " [SHA-256]" } else { "" });
                if let Some(error) = &j.last_error { println!("  error: {error}"); }
                if details {
                    for (route, stats) in j.route_stats { println!("  {route}: {} bytes, batch {} bytes, connect {} ms, session {} ms, failed {} ms, errors {}, reconnects {}", stats.bytes, if stats.chosen_batch_size > 0 { stats.chosen_batch_size } else { j.batch_size.max(j.chunk_size) }, stats.connect_ms, stats.session_ms, stats.failed_session_ms, stats.errors, stats.reconnects); }
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
        assert!(Cli::try_parse_from(["mssh", "--verify", "./a", "edge_a:/b"]).unwrap().verify);
        let tuned = Cli::try_parse_from(["mssh", "--chunk-size", "auto", "--min-chunk-size", "320KiB",
            "--max-chunk-size", "4MiB", "--route-chunk", "via-a=512KiB", "--chunk-timeout", "30",
            "./test.yml", "edge_a:/home"]).unwrap();
        assert_eq!(tuned.chunk_size.as_deref(), Some("auto"));
        assert_eq!(tuned.route_chunks, ["via-a=512KiB"]);
        assert_eq!(parse_size("320KiB").unwrap(), 320 * 1024);
        assert!(parse_size("300KB").is_err());
    }
}
