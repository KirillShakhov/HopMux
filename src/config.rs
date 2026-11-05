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
    pub routes: Vec<Route>,
    #[serde(skip)]
    pub route_filter: Vec<String>,
}

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
        let mut seen = BTreeSet::new();
        let mut config = Self::read_recursive(path, &mut seen)?;
        config.validate()?;
        config.routes.sort_by_key(|r| (r.priority, r.name.clone()));
        Ok(config)
    }

    fn read_recursive(path: &Path, seen: &mut BTreeSet<PathBuf>) -> Result<Self> {
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
            let child = Self::read_recursive(&canonical.parent().unwrap().join(include), seen)?;
            for (id, node) in child.nodes {
                ensure!(
                    config.nodes.insert(id.clone(), node).is_none(),
                    "duplicate node {id}"
                );
            }
            for route in child.routes {
                config.routes.push(route);
            }
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
        Ok(())
    }

    /// Nodes without a configured route can be reached directly. Explicit
    /// routes (including disabled ones) take precedence over this fallback.
    pub fn effective_routes(&self) -> Vec<Route> {
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
