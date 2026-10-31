use crate::config::{Config, Node, Route};
use anyhow::{Context, Result, bail};
use russh::{
    client,
    keys::{PrivateKeyWithHashAlg, PublicKeyOrCertificate},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, time::timeout};

#[derive(Clone)]
pub(crate) struct HostKeyPin {
    fingerprint: String,
}

impl client::Handler for HostKeyPin {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // Certificate trust requires CA and principal validation; pins apply only to raw keys.
        Ok(key.certificate().is_none()
            && key.public_key().fingerprint(Default::default()).to_string() == self.fingerprint)
    }
}

pub struct Connection {
    handles: Vec<client::Handle<HostKeyPin>>,
    pub elapsed: Duration,
}

impl Connection {
    pub fn final_handle(&self) -> &client::Handle<HostKeyPin> {
        self.handles.last().expect("route is nonempty")
    }
}

async fn authenticate(handle: &mut client::Handle<HostKeyPin>, node: &Node) -> Result<()> {
    if let Some(path) = &node.key_file {
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
        if handle
            .authenticate_publickey(&node.user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
            .await?
            .success()
        {
            return Ok(());
        }
    }
    if node.agent && authenticate_agent(handle, node).await? {
        return Ok(());
    }
    if let Some(name) = &node.password_env {
        let password = std::env::var(name)
            .with_context(|| format!("missing password environment variable {name}"))?;
        if handle
            .authenticate_password(&node.user, password)
            .await?
            .success()
        {
            return Ok(());
        }
    }
    bail!("authentication failed for {}@{}", node.user, node.host)
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
    bail!("ssh-agent authentication is supported on Linux")
}

pub async fn connect(config: &Config, route: &Route) -> Result<Connection> {
    let started = Instant::now();
    let duration = Duration::from_secs(config.settings.connect_timeout_secs);
    let ssh_config = Arc::new(client::Config {
        keepalive_interval: Some(Duration::from_secs(10)),
        keepalive_max: 3,
        ..Default::default()
    });
    let mut handles: Vec<client::Handle<HostKeyPin>> = Vec::new();
    for hop in &route.hops {
        let node = &config.nodes[hop];
        let host = route.addresses.get(hop).unwrap_or(&node.host);
        let pin = HostKeyPin {
            fingerprint: node.host_key.clone(),
        };
        let mut handle = if let Some(parent) = handles.last() {
            let channel = timeout(
                duration,
                parent.channel_open_direct_tcpip(host.clone(), node.port as u32, "127.0.0.1", 0),
            )
            .await
            .context("TCP forwarding timed out")?
            .with_context(|| format!("TCP forwarding refused for node {hop}"))?;
            timeout(
                duration,
                client::connect_stream(ssh_config.clone(), channel.into_stream(), pin),
            )
            .await
            .context("SSH handshake timed out")?
            .with_context(|| {
                format!("SSH handshake failed at node {hop} on route {}", route.name)
            })?
        } else {
            let stream = timeout(duration, TcpStream::connect((host.as_str(), node.port)))
                .await
                .context("TCP connection timed out")?
                .with_context(|| format!("TCP connection failed for node {hop}"))?;
            timeout(
                duration,
                client::connect_stream(ssh_config.clone(), stream, pin),
            )
            .await
            .context("SSH handshake timed out")?
            .with_context(|| {
                format!("SSH handshake failed at node {hop} on route {}", route.name)
            })?
        };
        timeout(duration, authenticate(&mut handle, node))
            .await
            .context("authentication timed out")?
            .with_context(|| format!("at node {hop} on route {}", route.name))?;
        handles.push(handle);
    }
    Ok(Connection {
        handles,
        elapsed: started.elapsed(),
    })
}
