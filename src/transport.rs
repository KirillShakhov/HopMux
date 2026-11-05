use crate::config::{Config, Node, Route};
use anyhow::{Context, Result};
use russh::{
    client,
    keys::{PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, time::timeout};

#[derive(Clone)]
pub(crate) struct HostKeyPin {
    fingerprint: Option<String>,
}

impl client::Handler for HostKeyPin {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // Certificate trust requires CA and principal validation; pins apply only to raw keys.
        let accepted = key.certificate().is_none()
            && self.fingerprint.as_ref().is_none_or(|fingerprint| {
                key.public_key().fingerprint(Default::default()).to_string() == *fingerprint
            });
        crate::debug_log!("SSH host key: {} ({})", if accepted { "accepted" } else { "rejected" }, if self.fingerprint.is_some() { "pinned" } else { "unpinned" });
        Ok(accepted)
    }
}

pub struct Connection {
    handles: Vec<client::Handle<HostKeyPin>>,
    pub elapsed: Duration,
    pub hop_timings: Vec<HopTiming>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HopTiming {
    pub node: String,
    pub connect_ms: u128,
}

impl Connection {
    pub fn final_handle(&self) -> &client::Handle<HostKeyPin> {
        self.handles.last().expect("route is nonempty")
    }
}

#[derive(Debug)]
struct AuthenticationFailure {
    node: String,
}

impl fmt::Display for AuthenticationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "authentication failed at node {}", self.node)
    }
}
impl StdError for AuthenticationFailure {}

pub fn is_permanent(error: &anyhow::Error) -> bool {
    failure_state(error) != "DOWN"
}

pub fn failure_state(error: &anyhow::Error) -> &'static str {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<russh::Error>()
            .is_some_and(|error| matches!(error, russh::Error::UnknownKey))
    }) {
        "HOST_KEY_MISMATCH"
    } else if error
        .chain()
        .any(|cause| cause.is::<AuthenticationFailure>())
    {
        "AUTH_FAILED"
    } else {
        "DOWN"
    }
}

async fn authenticate(handle: &mut client::Handle<HostKeyPin>, node: &Node) -> Result<()> {
    let mut method_errors = Vec::new();
    if let Some(path) = &node.key_file {
        crate::debug_log!("auth {}: trying key", node.host);
        let attempt: Result<bool> = async {
            let expanded = if let Some(rest) = path.strip_prefix("~/") {
                dirs::home_dir()
                    .context("cannot find home directory")?
                    .join(rest)
            } else {
                path.into()
            };
            let passphrase = node
                .key_passphrase_env
                .as_ref()
                .map(std::env::var)
                .transpose()
                .context("missing key passphrase environment variable")?;
            let key = russh::keys::load_secret_key(&expanded, passphrase.as_deref())
                .with_context(|| format!("cannot load key {}", expanded.display()))?;
            let hash = handle.best_supported_rsa_hash().await?.flatten();
            Ok(handle
                .authenticate_publickey(&node.user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
                .await?
                .success())
        }
        .await;
        match attempt {
            Ok(true) => { crate::debug_log!("auth {}: key accepted", node.host); return Ok(()) },
            Ok(false) => { crate::debug_log!("auth {}: key rejected", node.host); method_errors.push("key rejected".to_owned()) },
            Err(error) => { crate::debug_log!("auth {}: key unavailable: {error:#}", node.host); method_errors.push(format!("key unavailable: {error:#}")) },
        }
    }
    if node.agent {
        crate::debug_log!("auth {}: trying SSH agent", node.host);
        match authenticate_agent(handle, node).await {
            Ok(true) => { crate::debug_log!("auth {}: SSH agent accepted", node.host); return Ok(()) },
            Ok(false) => { crate::debug_log!("auth {}: SSH agent identities rejected", node.host); method_errors.push("agent identities rejected".to_owned()) },
            Err(error) => { crate::debug_log!("auth {}: SSH agent unavailable: {error:#}", node.host); method_errors.push(format!("agent unavailable: {error:#}")) },
        }
    }
    let password = if let Some(inline) = &node.password {
        Some(inline.clone())
    } else if let Some(name) = &node.password_env {
        match std::env::var(name) {
            Ok(password) => Some(password),
            Err(_) => {
                method_errors.push(format!("password environment variable {name} is missing"));
                None
            }
        }
    } else {
        None
    };
    if let Some(password) = password {
        crate::debug_log!("auth {}: trying password", node.host);
        if handle
            .authenticate_password(&node.user, password)
            .await?
            .success()
        {
            crate::debug_log!("auth {}: password accepted", node.host);
            return Ok(());
        }
        crate::debug_log!("auth {}: password rejected", node.host);
        method_errors.push("password rejected".to_owned());
    }
    Err(anyhow::Error::new(AuthenticationFailure {
        node: node.host.clone(),
    })
    .context(method_errors.join("; ")))
}

#[cfg(unix)]
async fn authenticate_agent(handle: &mut client::Handle<HostKeyPin>, node: &Node) -> Result<bool> {
    use russh::keys::agent::{AgentIdentity, client::AgentClient};
    let mut agent = AgentClient::connect_env()
        .await
        .context("cannot connect to ssh-agent")?;
    if let Some(binding) = handle.agent_session_binding() {
        agent
            .bind_session(binding, false)
            .await
            .context("ssh-agent session binding failed")?;
    }
    for identity in agent.request_identities().await? {
        if let AgentIdentity::PublicKey { key, .. } = identity {
            let hash = handle.best_supported_rsa_hash().await?.flatten();
            if handle
                .authenticate_publickey_with(&node.user, key, hash, &mut agent)
                .await?
                .success()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[cfg(not(unix))]
async fn authenticate_agent(_: &mut client::Handle<HostKeyPin>, _: &Node) -> Result<bool> {
    anyhow::bail!("ssh-agent authentication is supported on Linux")
}

pub async fn connect(config: &Config, route: &Route) -> Result<Connection> {
    crate::debug_log!("route {}: connecting to {} via {}", route.name, route.target, route.hops.join(" -> "));
    let started = Instant::now();
    let duration = Duration::from_secs(config.settings.connect_timeout_secs);
    let ssh_config = Arc::new(client::Config {
        keepalive_interval: Some(Duration::from_secs(10)),
        keepalive_max: 3,
        ..Default::default()
    });
    let mut handles: Vec<client::Handle<HostKeyPin>> = Vec::new();
    let mut hop_timings = Vec::new();
    for hop in &route.hops {
        let hop_started = Instant::now();
        let node = &config.nodes[hop];
        let host = route.addresses.get(hop).unwrap_or(&node.host);
        crate::debug_log!("route {}: hop {} connecting to {}:{} ({})", route.name, hop, host, node.port, if handles.is_empty() { "direct" } else { "forwarded" });
        let pin = HostKeyPin {
            fingerprint: node.host_key.clone(),
        };
        let mut handle = if let Some(parent) = handles.last() {
            let channel = timeout(
                duration,
                parent.channel_open_direct_tcpip(host.clone(), node.port as u32, "127.0.0.1", 0),
            )
            .await
            .with_context(|| {
                format!("TCP forwarding timed out at node {hop} on route {}", route.name)
            })?
            .with_context(|| format!("TCP forwarding refused for node {hop}"))?;
            timeout(
                duration,
                client::connect_stream(ssh_config.clone(), channel.into_stream(), pin),
            )
            .await
            .with_context(|| {
                format!("SSH handshake timed out at node {hop} on route {}", route.name)
            })?
            .with_context(|| {
                format!("SSH handshake failed at node {hop} on route {}", route.name)
            })?
        } else {
            let stream = timeout(duration, TcpStream::connect((host.as_str(), node.port)))
                .await
                .with_context(|| {
                    format!("TCP connection timed out at node {hop} on route {}", route.name)
                })?
                .with_context(|| format!("TCP connection failed for node {hop}"))?;
            timeout(
                duration,
                client::connect_stream(ssh_config.clone(), stream, pin),
            )
            .await
            .with_context(|| {
                format!("SSH handshake timed out at node {hop} on route {}", route.name)
            })?
            .with_context(|| {
                format!("SSH handshake failed at node {hop} on route {}", route.name)
            })?
        };
        crate::debug_log!("route {}: hop {} SSH handshake complete", route.name, hop);
        timeout(duration, authenticate(&mut handle, node))
            .await
            .with_context(|| {
                format!("authentication timed out at node {hop} on route {}", route.name)
            })?
            .with_context(|| format!("at node {hop} on route {}", route.name))?;
        handles.push(handle);
        crate::debug_log!("route {}: hop {} authenticated in {} ms", route.name, hop, hop_started.elapsed().as_millis());
        hop_timings.push(HopTiming {
            node: hop.clone(),
            connect_ms: hop_started.elapsed().as_millis(),
        });
    }
    crate::debug_log!("route {}: connected in {} ms", route.name, started.elapsed().as_millis());
    Ok(Connection {
        handles,
        elapsed: started.elapsed(),
        hop_timings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_errors_are_not_retried() {
        assert!(is_permanent(&anyhow::Error::new(AuthenticationFailure {
            node: "edge".into()
        })));
        assert!(is_permanent(
            &anyhow::Error::new(AuthenticationFailure {
                node: "edge".into()
            })
            .context("route one")
        ));
        assert!(is_permanent(&anyhow::Error::new(russh::Error::UnknownKey)));
        assert!(!is_permanent(&anyhow::anyhow!("connection timed out")));
    }
}
