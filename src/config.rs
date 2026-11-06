use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub settings: Settings,
    #[serde(default)]
    pub nodes: BTreeMap<String, Node>,
    #[serde(default)]
    pub forwards: BTreeMap<String, Forward>,
    #[serde(default)]
    pub direct_hosts: Vec<String>,
    #[serde(default)]
    pub paths: Vec<Vec<String>>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(skip)]
    pub route_filter: Vec<String>,
    #[serde(skip, default = "default_auto_chunk")]
    pub auto_chunk: bool,
    #[serde(skip, default = "default_min_chunk")]
    pub min_chunk_size: usize,
    #[serde(skip, default = "default_max_chunk")]
    pub max_chunk_size: usize,
    #[serde(skip)]
    pub chunk_timeout_secs: Option<u64>,
    #[serde(skip)]
    pub route_chunk_sizes: BTreeMap<String, usize>,
    #[serde(skip)]
    pub verify: bool,
}

fn default_auto_chunk() -> bool { true }
fn default_min_chunk() -> usize { 256 * 1024 }
fn default_max_chunk() -> usize { 8 * 1024 * 1024 }

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default = "default_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_interval")]
    pub check_interval_secs: u64,
    #[serde(default = "default_parallel")]
    pub max_parallel_routes: usize,
    #[serde(default = "default_chunk")]
    pub chunk_size: usize,
}
fn default_timeout() -> u64 {
    10
}
fn default_interval() -> u64 {
    30
}
fn default_parallel() -> usize {
    3
}
fn default_chunk() -> usize {
    1024 * 1024
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            connect_timeout_secs: default_timeout(),
            check_interval_secs: default_interval(),
            max_parallel_routes: default_parallel(),
            chunk_size: default_chunk(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub local_ssh_port: Option<u16>,
    pub user: String,
    pub host_key: Option<String>,
    pub key_file: Option<String>,
    pub key_passphrase_env: Option<String>,
    pub password_env: Option<String>,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    #[serde(default)]
    pub agent: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Forward {
    pub via: String,
    pub listen: String,
    pub remote: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingDestination {
    Ssh,
    Service { host: String, port: u16 },
}

#[derive(Debug, Clone)]
pub struct PortBinding {
    pub name: String,
    pub via: String,
    pub listen: std::net::SocketAddr,
    pub destination: BindingDestination,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Node")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("host_key", &self.host_key)
            .field("key_file", &self.key_file)
            .field("key_passphrase_env", &self.key_passphrase_env)
            .field("password_env", &self.password_env)
            .field("password_configured", &self.password.is_some())
            .field("agent", &self.agent)
            .finish()
    }
}
fn default_port() -> u16 {
    22
}

fn split_host_port(value: &str) -> Result<(String, u16)> {
    let (host, port) = value.rsplit_once(':').context("expected HOST:PORT")?;
    let host = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        ensure!(!host.contains(':'), "IPv6 addresses must be bracketed");
        host
    };
    ensure!(!host.is_empty() && !host.chars().any(char::is_whitespace), "host is empty or contains whitespace");
    let port = port.parse::<u16>().context("invalid port")?;
    ensure!(port > 0, "port must be positive");
    Ok((host.to_owned(), port))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub name: String,
    pub target: String,
    pub hops: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub addresses: BTreeMap<String, String>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        crate::debug_log!("config: reading {}", path.display());
        let mut seen = BTreeSet::new();
        let mut config = Self::read_recursive(path, &mut seen)?;
        crate::debug_log!("config: validating {} nodes, {} paths, {} direct hosts and {} legacy routes", config.nodes.len(), config.paths.len(), config.direct_hosts.len(), config.routes.len());
        config.validate()?;
        config.routes.sort_by_key(|r| (r.priority, r.name.clone()));
        Ok(config)
    }

    fn read_recursive(path: &Path, seen: &mut BTreeSet<PathBuf>) -> Result<Self> {
        crate::debug_log!("config: opening {}", path.display());
        let canonical = path
            .canonicalize()
            .with_context(|| format!("cannot open config {}", path.display()))?;
        ensure!(
            seen.insert(canonical.clone()),
            "config include cycle or duplicate: {}",
            canonical.display()
        );
        let contents = fs::read_to_string(&canonical)?;
        let mut config: Config = toml::from_str(&contents).map_err(|error: toml::de::Error| {
            let location = error.span().map_or(String::new(), |span| {
                let line = contents.as_bytes()[..span.start.min(contents.len())]
                    .iter()
                    .filter(|&&byte| byte == b'\n')
                    .count()
                    + 1;
                format!(" at line {line}")
            });
            anyhow::anyhow!(
                "invalid TOML in {}{location}; check syntax and field types",
                canonical.display()
            )
        })?;
        let includes = std::mem::take(&mut config.include);
        for include in includes {
            crate::debug_log!("config: processing include {include}");
            let child = Self::read_recursive(&canonical.parent().unwrap().join(include), seen)?;
            for (id, node) in child.nodes {
                ensure!(
                    config.nodes.insert(id.clone(), node).is_none(),
                    "duplicate node {id}"
                );
            }
            for (name, forward) in child.forwards {
                ensure!(config.forwards.insert(name.clone(), forward).is_none(), "duplicate forward {name}");
            }
            for route in child.routes {
                config.routes.push(route);
            }
            config.direct_hosts.extend(child.direct_hosts);
            config.paths.extend(child.paths);
        }
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.nodes.is_empty(), "no nodes configured");
        ensure!(
            self.settings.connect_timeout_secs > 0 && self.settings.check_interval_secs > 0,
            "timeouts and check intervals must be positive"
        );
        ensure!(
            (1..=16).contains(&self.settings.max_parallel_routes),
            "max_parallel_routes must be 1..=16"
        );
        ensure!(
            (64 * 1024..=8 * 1024 * 1024).contains(&self.settings.chunk_size),
            "chunk_size must be between 64 KiB and 8 MiB"
        );
        for (id, node) in &self.nodes {
            ensure!(
                !id.is_empty() && !node.host.is_empty() && !node.user.is_empty(),
                "node {id}: id, host and user are required"
            );
            ensure!(node.port > 0 && node.local_ssh_port.is_none_or(|port| port > 0),
                "node {id}: SSH ports must be positive");
            if let Some(host_key) = &node.host_key {
                ensure!(
                    host_key.starts_with("SHA256:")
                        && host_key
                            .parse::<russh::keys::ssh_key::Fingerprint>()
                            .is_ok(),
                    "node {id}: host_key must be a SHA256 fingerprint"
                );
            }
            ensure!(
                node.key_file.is_some()
                    || node.password_env.is_some()
                    || node.password.is_some()
                    || node.agent,
                "node {id}: configure key_file, password, password_env or agent"
            );
            ensure!(
                node.password.is_none() || node.password_env.is_none(),
                "node {id}: choose password or password_env, not both"
            );
            if let Some(password) = &node.password {
                ensure!(
                    !password.is_empty(),
                    "node {id}: password must not be empty"
                );
            }
            for name in [&node.password_env, &node.key_passphrase_env]
                .into_iter()
                .flatten()
            {
                ensure!(
                    name.chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                        && !name.is_empty(),
                    "node {id}: invalid environment variable name"
                );
            }
        }
        if self.graph_mode() {
            ensure!(self.routes.is_empty(), "use paths and direct_hosts without legacy [[routes]] entries");
            ensure!(!self.direct_hosts.is_empty(), "direct_hosts must list at least one host reachable from the client");
            let mut direct_seen = BTreeSet::new();
            for host in &self.direct_hosts {
                ensure!(self.nodes.contains_key(host), "direct_hosts: unknown node {host}");
                ensure!(direct_seen.insert(host), "direct_hosts: duplicate node {host}");
            }
            for (index, path) in self.paths.iter().enumerate() {
                ensure!(path.len() >= 2, "paths[{}] must contain at least two hosts", index + 1);
                let mut visited = BTreeSet::new();
                for host in path {
                    ensure!(self.nodes.contains_key(host), "paths[{}]: unknown node {host}", index + 1);
                    ensure!(visited.insert(host), "paths[{}]: repeated node {host}", index + 1);
                }
            }
            let (routes, overflow) = crate::routing::discover(self);
            ensure!(!overflow, "too many paths can be composed; simplify the network graph");
            let reachable: BTreeSet<_> = routes.iter().map(|route| route.target.as_str()).collect();
            for path in &self.paths {
                ensure!(path.iter().all(|host| reachable.contains(host.as_str())),
                    "configured path contains a host unreachable from direct_hosts: {}", path.join(" -> "));
            }
            self.bindings()?;
            return Ok(());
        }
        let mut names = BTreeSet::new();
        for route in &self.routes {
            ensure!(names.insert(&route.name), "duplicate route {}", route.name);
            ensure!(
                !route.name.is_empty() && !route.hops.is_empty(),
                "route {}: name and hops are required",
                route.name
            );
            ensure!(
                route.hops.last() == Some(&route.target),
                "route {}: last hop must be target",
                route.name
            );
            let mut visited = BTreeSet::new();
            for hop in &route.hops {
                ensure!(
                    self.nodes.contains_key(hop),
                    "route {}: unknown node {hop}",
                    route.name
                );
                ensure!(
                    visited.insert(hop),
                    "route {}: repeated node {hop}",
                    route.name
                );
            }
            for key in route.addresses.keys() {
                ensure!(
                    visited.contains(key),
                    "route {}: address override for node outside route: {key}",
                    route.name
                );
            }
        }
        if self.effective_routes().iter().all(|r| r.disabled) {
            bail!("all routes are disabled");
        }
        self.bindings()?;
        Ok(())
    }

    pub fn bindings(&self) -> Result<Vec<PortBinding>> {
        let mut bindings = Vec::new();
        for (node, settings) in &self.nodes {
            if let Some(port) = settings.local_ssh_port {
                bindings.push(PortBinding {
                    name: format!("ssh:{node}"), via: node.clone(),
                    listen: format!("127.0.0.1:{port}").parse()?,
                    destination: BindingDestination::Ssh,
                });
            }
        }
        for (name, forward) in &self.forwards {
            ensure!(!name.is_empty(), "forward name must not be empty");
            ensure!(self.nodes.contains_key(&forward.via), "forward {name}: unknown node {}", forward.via);
            let listen = forward.listen.parse::<std::net::SocketAddr>()
                .with_context(|| format!("forward {name}: listen must be an IP address and port"))?;
            ensure!(listen.port() > 0, "forward {name}: listen port must be positive");
            let (host, port) = split_host_port(&forward.remote)
                .with_context(|| format!("forward {name}: invalid remote address"))?;
            bindings.push(PortBinding {
                name: name.clone(), via: forward.via.clone(), listen,
                destination: BindingDestination::Service { host, port },
            });
        }
        for (index, binding) in bindings.iter().enumerate() {
            ensure!(!self.routes_for(&binding.via).is_empty(),
                "binding {}: no route to {}", binding.name, binding.via);
            for previous in &bindings[..index] {
                if previous.listen.port() == binding.listen.port()
                    && previous.listen.is_ipv4() == binding.listen.is_ipv4()
                    && (previous.listen.ip() == binding.listen.ip()
                        || previous.listen.ip().is_unspecified()
                        || binding.listen.ip().is_unspecified()) {
                    bail!("bindings {} and {} overlap on local port {}", previous.name, binding.name, binding.listen.port());
                }
            }
        }
        Ok(bindings)
    }

    /// Nodes without a configured route can be reached directly. Explicit
    /// routes (including disabled ones) take precedence over this fallback.
    pub fn effective_routes(&self) -> Vec<Route> {
        if self.graph_mode() { return crate::routing::discover(self).0; }
        let mut routes = self.routes.clone();
        let mut names: BTreeSet<String> = routes.iter().map(|r| r.name.clone()).collect();
        for id in self.nodes.keys() {
            if routes.iter().any(|r| r.target == *id) {
                continue;
            }
            let base = format!("direct:{id}");
            let mut name = base.clone();
            let mut suffix = 2;
            while names.contains(&name) {
                name = format!("{base}:{suffix}");
                suffix += 1;
            }
            names.insert(name.clone());
            routes.push(Route {
                name,
                target: id.clone(),
                hops: vec![id.clone()],
                priority: 0,
                disabled: false,
                addresses: BTreeMap::new(),
            });
        }
        routes
    }

    pub fn graph_mode(&self) -> bool {
        !self.direct_hosts.is_empty() || !self.paths.is_empty()
    }

    pub fn routes_for(&self, target: &str) -> Vec<Route> {
        self.effective_routes()
            .into_iter()
            .filter(|r| !r.disabled && r.target == target &&
                (self.route_filter.is_empty() || self.route_filter.contains(&r.name)))
            .collect()
    }
}

pub fn default_path() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("cannot find config directory")?
        .join("mssh/config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_ssh_and_services_through_discovered_routes() {
        let mut config = graph_sample();
        config.nodes.get_mut("target").unwrap().local_ssh_port = Some(2222);
        config.forwards.insert("postgres".into(), Forward {
            via: "target".into(), listen: "127.0.0.1:15432".into(),
            remote: "127.0.0.1:5432".into(),
        });
        config.validate().unwrap();
        let bindings = config.bindings().unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].name, "ssh:target");
        assert_eq!(bindings[0].listen.port(), 2222);
        assert_eq!(bindings[1].destination, BindingDestination::Service {
            host: "127.0.0.1".into(), port: 5432,
        });
        assert_eq!(config.routes_for(&bindings[0].via).len(), 2);
    }

    #[test]
    fn rejects_overlapping_listeners_and_unreachable_via() {
        let mut config = graph_sample();
        config.nodes.get_mut("target").unwrap().local_ssh_port = Some(2222);
        config.forwards.insert("duplicate".into(), Forward {
            via: "target".into(), listen: "0.0.0.0:2222".into(), remote: "127.0.0.1:80".into(),
        });
        assert!(config.validate().unwrap_err().to_string().contains("overlap"));
        config.forwards.get_mut("duplicate").unwrap().listen = "127.0.0.1:18080".into();
        config.forwards.get_mut("duplicate").unwrap().via = "missing".into();
        assert!(config.validate().unwrap_err().to_string().contains("unknown node"));
    }

    fn graph_sample() -> Config {
        toml::from_str(r#"
            direct_hosts = ["edge_a", "edge_b"]
            paths = [
                ["edge_a", "relay"],
                ["relay", "hub"],
                ["edge_b", "hub"],
                ["hub", "target"],
            ]
            [nodes.edge_a]
            host = "edge-a"
            user = "alice"
            key_file = "key"
            [nodes.edge_b]
            host = "edge-b"
            user = "alice"
            key_file = "key"
            [nodes.relay]
            host = "relay"
            user = "alice"
            key_file = "key"
            [nodes.hub]
            host = "hub"
            user = "alice"
            key_file = "key"
            [nodes.target]
            host = "target"
            user = "alice"
            key_file = "key"
        "#).unwrap()
    }

    #[test]
    fn composes_short_paths_in_list_priority_order() {
        let config = graph_sample();
        config.validate().unwrap();
        let routes = config.routes_for("target");
        assert_eq!(routes.iter().map(|route| route.hops.as_slice()).collect::<Vec<_>>(), [
            &["edge_a", "relay", "hub", "target"][..],
            &["edge_b", "hub", "target"][..],
        ]);
        assert_eq!(routes.iter().map(|route| route.name.as_str()).collect::<Vec<_>>(),
            ["target-1", "target-2"]);
        assert_eq!(config.routes_for("hub")[0].hops, ["edge_a", "relay", "hub"]);
        assert_eq!(config.routes_for("relay")[0].hops, ["edge_a", "relay"]);
        assert_eq!(config.routes_for("edge_a")[0].hops, ["edge_a"]);
        assert!(config.routes_for("missing").is_empty());
    }

    #[test]
    fn path_order_controls_priority_even_when_direct_host_order_differs() {
        let mut config = graph_sample();
        config.paths.swap(0, 2);
        config.validate().unwrap();
        assert_eq!(config.routes_for("target")[0].hops, ["edge_b", "hub", "target"]);
        assert_eq!(config.routes_for("target")[1].hops,
            ["edge_a", "relay", "hub", "target"]);
    }

    #[test]
    fn long_path_exposes_every_prefix() {
        let mut config = graph_sample();
        config.paths = vec![vec!["edge_a".into(), "relay".into(), "hub".into(), "target".into()]];
        config.validate().unwrap();
        assert_eq!(config.routes_for("relay")[0].hops, ["edge_a", "relay"]);
        assert_eq!(config.routes_for("hub")[0].hops, ["edge_a", "relay", "hub"]);
        assert_eq!(config.routes_for("target")[0].hops, ["edge_a", "relay", "hub", "target"]);
        assert!(config.routes_for("edge_b").len() == 1);
    }

    #[test]
    fn graph_requires_reachable_declared_hosts() {
        let mut config = graph_sample();
        config.paths = vec![vec!["relay".into(), "hub".into()]];
        assert!(config.validate().unwrap_err().to_string().contains("unreachable"));
        config.paths = vec![vec!["edge_a".into(), "missing".into()]];
        assert!(config.validate().unwrap_err().to_string().contains("unknown node"));
    }

    fn sample() -> Config {
        toml::from_str(
            r#"
            [nodes.edge]
            host = "edge.example"
            user = "alice"
            host_key = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            key_file = "~/.ssh/id_ed25519"
            [nodes.internal]
            host = "10.0.0.5"
            user = "alice"
            host_key = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            password_env = "MSSH_PASSWORD"
            [[routes]]
            name = "route-one"
            target = "internal"
            hops = ["edge", "internal"]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn accepts_valid_chain() {
        sample().validate().unwrap();
    }

    #[test]
    fn entry_node_without_route_connects_directly() {
        let config = sample();
        let routes = config.routes_for("edge");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops, ["edge"]);
        assert_eq!(routes[0].name, "direct:edge");
        assert_eq!(config.routes_for("internal")[0].hops, ["edge", "internal"]);
        assert!(config.routes_for("missing").is_empty());
    }

    #[test]
    fn nodes_only_configuration_is_valid() {
        let mut config = sample();
        config.routes.clear();
        config.validate().unwrap();
        assert_eq!(config.routes_for("internal")[0].hops, ["internal"]);
    }

    #[test]
    fn disabled_route_does_not_become_direct() {
        let mut config = sample();
        config.routes[0].disabled = true;
        config.validate().unwrap(); // the entry node still has a direct route
        assert!(config.routes_for("internal").is_empty());
    }

    #[test]
    fn explicit_entry_route_overrides_direct_fallback() {
        let mut config = sample();
        config.routes.push(Route {
            name: "via-other".into(),
            target: "edge".into(),
            hops: vec!["internal".into(), "edge".into()],
            priority: 0,
            disabled: false,
            addresses: BTreeMap::new(),
        });
        config.validate().unwrap();
        assert_eq!(config.routes_for("edge")[0].name, "via-other");
        assert_eq!(config.routes_for("edge").len(), 1);
    }

    #[test]
    fn generated_names_do_not_collide_with_explicit_routes() {
        let mut config = sample();
        config.routes[0].name = "direct:edge".into();
        config.validate().unwrap();
        assert_eq!(config.routes_for("edge")[0].name, "direct:edge:2");
    }

    #[test]
    fn rejects_unknown_and_repeated_hops() {
        let mut config = sample();
        config.routes[0].hops[0] = "missing".into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("unknown node")
        );
        config.routes[0].hops[0] = "internal".into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("repeated node")
        );
    }

    #[test]
    fn accepts_missing_host_key() {
        let mut config = sample();
        config.nodes.get_mut("edge").unwrap().host_key = None;
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_host_key() {
        let mut config = sample();
        config.nodes.get_mut("edge").unwrap().host_key = Some("invalid".into());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("host_key")
        );
    }

    #[test]
    fn inline_password_is_usable_and_redacted() {
        let mut config = sample();
        let node = config.nodes.get_mut("internal").unwrap();
        node.password_env = None;
        node.password = Some("private-test-password".into());
        config.validate().unwrap();
        assert!(!format!("{config:?}").contains("private-test-password"));
        assert!(
            !toml::to_string(&config)
                .unwrap()
                .contains("private-test-password")
        );
    }

    #[test]
    fn rejects_conflicting_password_sources() {
        let mut config = sample();
        config.nodes.get_mut("internal").unwrap().password = Some("secret".into());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("choose password or password_env")
        );
    }

    #[test]
    fn parse_errors_do_not_reveal_inline_passwords() {
        let path = std::env::temp_dir().join(format!("mssh-invalid-{}.toml", uuid::Uuid::new_v4()));
        fs::write(
            &path,
            "[nodes.edge]\npassword = \"private-test-password\" invalid\n",
        )
        .unwrap();
        let error = Config::load(&path).unwrap_err();
        fs::remove_file(&path).unwrap();
        let message = format!("{error:#}");
        assert!(message.contains("invalid TOML"));
        assert!(message.contains("line 2"));
        assert!(!message.contains("private-test-password"));
    }

    #[test]
    fn bounds_parallel_memory() {
        let mut config = sample();
        config.settings.chunk_size = 64 * 1024 * 1024;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("chunk_size")
        );
        config.settings.chunk_size = 1024 * 1024;
        config.settings.max_parallel_routes = 100;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_parallel_routes")
        );
    }
}
