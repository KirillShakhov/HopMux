use crate::{config::{BindingDestination, Config, PortBinding, Route}, output, transport};
use anyhow::{Context, Result, bail, ensure};
use std::{sync::Arc, time::Duration};
use tokio::{io::{AsyncRead, AsyncWrite, copy_bidirectional}, net::{TcpListener, TcpStream}, sync::{Semaphore, mpsc, watch}, task::JoinSet, time::{Instant, MissedTickBehavior, timeout}};

const HEALTH_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_WAIT: Duration = Duration::from_secs(45);
const MAX_CLIENTS_PER_BINDING: usize = 256;

#[derive(Clone)]
enum Tunnel {
    Direct,
    Ssh(Arc<transport::Connection>),
}

struct LiveRoute {
    generation: u64,
    route: Route,
    tunnel: Tunnel,
    remote_host: String,
    remote_port: u16,
}

pub async fn serve(config: &Config) -> Result<()> {
    let bindings = config.bindings()?;
    ensure!(!bindings.is_empty(), "no port bindings configured");
    let mut listeners = Vec::new();
    for binding in bindings {
        let listener = TcpListener::bind(binding.listen).await
            .with_context(|| format!("binding {}: cannot listen on {}", binding.name, binding.listen))?;
        listeners.push((binding, listener));
    }
    let config = Arc::new(config.clone());
    let mut tasks = JoinSet::new();
    for (binding, listener) in listeners {
        let remote = remote_label(&config, &binding);
        eprintln!("forward {}: {} -> {} -> {remote}", binding.name, binding.listen, binding.via);
        tasks.spawn(serve_binding(config.clone(), binding, listener));
    }
    while let Some(result) = tasks.join_next().await {
        result.context("forward listener task failed")??;
    }
    bail!("all forward listeners stopped")
}

fn remote_label(config: &Config, binding: &PortBinding) -> String {
    match &binding.destination {
        BindingDestination::Ssh => {
            let node = &config.nodes[&binding.via];
            format!("{}:{}", node.host, node.port)
        }
        BindingDestination::Service { host, port } => format!("{host}:{port}"),
    }
}

async fn serve_binding(config: Arc<Config>, binding: PortBinding, listener: TcpListener) -> Result<()> {
    let routes = config.routes_for(&binding.via);
    ensure!(!routes.is_empty(), "binding {}: no routes", binding.name);
    let (ready_tx, ready_rx) = watch::channel::<Option<Arc<LiveRoute>>>(None);
    let (fault_tx, fault_rx) = mpsc::unbounded_channel();
    let mut manager = tokio::spawn(route_manager(config.clone(), binding.clone(), routes, ready_tx, fault_rx));
    let permits = Arc::new(Semaphore::new(MAX_CLIENTS_PER_BINDING));
    loop {
        let (client, _) = tokio::select! {
            result = listener.accept() => result.with_context(|| format!("binding {}: accept failed", binding.name))?,
            result = &mut manager => {
                result.context("route manager failed")?;
                bail!("binding {}: route manager stopped", binding.name);
            }
        };
        let permit = permits.clone().acquire_owned().await?;
        let config = config.clone();
        let client_binding = binding.clone();
        let ready_rx = ready_rx.clone();
        let fault_tx = fault_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = forward_client(&config, &client_binding, client, ready_rx, fault_tx).await {
                crate::debug_log!("forward {}: connection failed: {error:#}", client_binding.name);
            }
        });
    }
}

async fn route_manager(
    config: Arc<Config>, binding: PortBinding, routes: Vec<Route>,
    ready_tx: watch::Sender<Option<Arc<LiveRoute>>>, mut fault_rx: mpsc::UnboundedReceiver<u64>,
) {
    let mut next_route = 0;
    let mut generation = 0;
    let mut delay = Duration::from_secs(1);
    loop {
        let mut selected = None;
        for offset in 0..routes.len() {
            let index = (next_route + offset) % routes.len();
            let route = &routes[index];
            match connect_route(&config, &binding, route).await {
                Ok(mut live) => {
                    generation += 1;
                    live.generation = generation;
                    selected = Some((index, Arc::new(live)));
                    break;
                }
                Err(error) => crate::debug_log!("forward {}: {} unavailable: {error:#}", binding.name, output::route_label(route)),
            }
        }
        if let Some((index, live)) = selected {
            next_route = (index + 1) % routes.len();
            delay = Duration::from_secs(1);
            crate::debug_log!("forward {}: ready {} -> {} -> {}:{}", binding.name, binding.listen,
                output::route_chain(&live.route.hops), live.remote_host, live.remote_port);
            ready_tx.send_replace(Some(live.clone()));
            let mut checks = tokio::time::interval_at(Instant::now() + HEALTH_INTERVAL, HEALTH_INTERVAL);
            checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = checks.tick() => {
                        if !route_healthy(&live).await {
                            crate::debug_log!("forward {}: route unhealthy {}", binding.name, output::route_label(&live.route));
                            break;
                        }
                    }
                    fault = fault_rx.recv() => {
                        if fault == Some(live.generation) {
                            crate::debug_log!("forward {}: route failed on new connection {}", binding.name, output::route_label(&live.route));
                            break;
                        }
                    }
                }
            }
            ready_tx.send_replace(None);
        } else {
            ready_tx.send_replace(None);
            crate::debug_log!("forward {}: all routes unavailable; retry in {}s", binding.name, delay.as_secs());
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

async fn connect_route(config: &Config, binding: &PortBinding, route: &Route) -> Result<LiveRoute> {
    let (remote_host, remote_port, tunnel) = match &binding.destination {
        BindingDestination::Service { host, port } => {
            let connection = transport::connect(config, route).await?;
            (host.clone(), *port, Tunnel::Ssh(Arc::new(connection)))
        }
        BindingDestination::Ssh => {
            let node = &config.nodes[&binding.via];
            let host = route.addresses.get(&binding.via).unwrap_or(&node.host).clone();
            if route.hops.len() == 1 {
                timeout(Duration::from_secs(config.settings.connect_timeout_secs), TcpStream::connect((host.as_str(), node.port)))
                    .await.context("direct SSH port probe timed out")??;
                (host, node.port, Tunnel::Direct)
            } else {
                let mut parent = route.clone();
                parent.hops.pop();
                parent.target = parent.hops.last().unwrap().clone();
                let connection = transport::connect(config, &parent).await?;
                let channel = timeout(Duration::from_secs(config.settings.connect_timeout_secs),
                    connection.final_handle().channel_open_direct_tcpip(host.clone(), node.port as u32, "127.0.0.1", 0))
                    .await.context("SSH port probe timed out")??;
                drop(channel);
                (host, node.port, Tunnel::Ssh(Arc::new(connection)))
            }
        }
    };
    Ok(LiveRoute { generation: 0, route: route.clone(), tunnel, remote_host, remote_port })
}

async fn route_healthy(live: &LiveRoute) -> bool {
    match &live.tunnel {
        Tunnel::Direct => timeout(Duration::from_secs(5), TcpStream::connect((live.remote_host.as_str(), live.remote_port)))
            .await.is_ok_and(|result| result.is_ok()),
        Tunnel::Ssh(connection) => timeout(Duration::from_secs(5), connection.final_handle().send_ping())
            .await.is_ok_and(|result| result.is_ok()),
    }
}

async fn forward_client(
    config: &Config, binding: &PortBinding, mut client: TcpStream,
    mut ready_rx: watch::Receiver<Option<Arc<LiveRoute>>>, fault_tx: mpsc::UnboundedSender<u64>,
) -> Result<()> {
    let started = Instant::now();
    let mut attempts = 0;
    loop {
        let live = loop {
            if let Some(live) = ready_rx.borrow_and_update().clone() { break live; }
            let remaining = CLIENT_WAIT.saturating_sub(started.elapsed());
            ensure!(!remaining.is_zero(), "no route became available within {}s", CLIENT_WAIT.as_secs());
            timeout(remaining, ready_rx.changed()).await.context("waiting for a route timed out")??;
        };
        attempts += 1;
        let opened: Result<()> = match &live.tunnel {
            Tunnel::Direct => {
                let result = timeout(Duration::from_secs(config.settings.connect_timeout_secs),
                    TcpStream::connect((live.remote_host.as_str(), live.remote_port))).await;
                match result {
                    Ok(Ok(mut remote)) => {
                        crate::debug_log!("forward {}: {} -> {} -> {}:{}", binding.name, binding.listen,
                            output::route_chain(&live.route.hops), live.remote_host, live.remote_port);
                        return pipe(&mut client, &mut remote).await;
                    }
                    Ok(Err(error)) => Err(anyhow::Error::new(error)),
                    Err(error) => Err(anyhow::Error::new(error)),
                }
            }
            Tunnel::Ssh(connection) => {
                let result = timeout(Duration::from_secs(config.settings.connect_timeout_secs),
                    connection.final_handle().channel_open_direct_tcpip(
                        live.remote_host.clone(), live.remote_port as u32, "127.0.0.1", 0)).await;
                match result {
                    Ok(Ok(channel)) => {
                        let mut remote = channel.into_stream();
                        crate::debug_log!("forward {}: {} -> {} -> {}:{}", binding.name, binding.listen,
                            output::route_chain(&live.route.hops), live.remote_host, live.remote_port);
                        return pipe(&mut client, &mut remote).await;
                    }
                    Ok(Err(error)) => Err(anyhow::Error::new(error)),
                    Err(error) => Err(anyhow::Error::new(error)),
                }
            }
        };
        if let Err(error) = opened {
            crate::debug_log!("forward {}: opening via {} failed: {error:#}", binding.name, output::route_label(&live.route));
            let _ = fault_tx.send(live.generation);
            if attempts >= 2 { return Err(error); }
            let remaining = CLIENT_WAIT.saturating_sub(started.elapsed());
            ensure!(!remaining.is_zero(), "no alternate route became available");
            timeout(remaining, ready_rx.changed()).await.context("waiting for alternate route timed out")??;
        }
    }
}

async fn pipe<R: AsyncRead + AsyncWrite + Unpin>(client: &mut TcpStream, remote: &mut R) -> Result<()> {
    let (to_remote, to_local) = copy_bidirectional(client, remote).await?;
    crate::debug_log!("forward TCP stream ended: sent {to_remote} bytes, received {to_local} bytes");
    Ok(())
}
