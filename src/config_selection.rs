use crate::config::Config;
use anyhow::{Context, Result, bail, ensure};
use std::{
    collections::BTreeSet,
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};

fn remote_host(value: &str) -> Option<&str> {
    let (host, path) = value.split_once(':')?;
    (!host.is_empty() && path.starts_with('/')).then_some(host)
}

fn first_host(args: &[String]) -> Option<&str> {
    match args {
        [command, target] if command == "status" || command == "transfers" => Some(target),
        [command, check] if command == "config" && check == "check" => None,
        [command] if command == "status" || command == "serve" => None,
        [target] => Some(target),
        [source, destination] => remote_host(source).or_else(|| remote_host(destination)),
        _ => None,
    }
}

fn config_files(location: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(location)
        .with_context(|| format!("cannot read config directory {}", location.display()))?
    {
        let path = entry?.path();
        if path.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"))
        {
            files.push(path);
        }
    }
    files.sort();
    let mut included = BTreeSet::new();
    for file in &files {
        let contents = fs::read_to_string(file)
            .with_context(|| format!("cannot read config {}", file.display()))?;
        let table: toml::Table = toml::from_str(&contents).map_err(|_| {
            anyhow::anyhow!(
                "invalid TOML in {}; check syntax and field types",
                file.display()
            )
        })?;
        if let Some(paths) = table.get("include").and_then(toml::Value::as_array) {
            for path in paths {
                if let Some(path) = path.as_str() {
                    let included_path = file.parent().unwrap().join(path);
                    if let Ok(canonical) = included_path.canonicalize() {
                        included.insert(canonical);
                    }
                }
            }
        }
    }
    files.retain(|file| {
        !file
            .canonicalize()
            .is_ok_and(|path| included.contains(&path))
    });
    ensure!(
        !files.is_empty(),
        "no TOML config files in {}",
        location.display()
    );
    Ok(files)
}

fn matching_files(files: &[PathBuf], host: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut matches = Vec::new();
    for file in files {
        let config =
            Config::load(file).with_context(|| format!("while inspecting {}", file.display()))?;
        if host.is_none_or(|host| config.resolve_node_name(host).is_some()) {
            matches.push(file.clone());
        }
    }
    if let Some(host) = host {
        ensure!(!matches.is_empty(), "no config contains node {host:?}");
    }
    Ok(matches)
}

pub fn select_path(location: &Path, args: &[String]) -> Result<PathBuf> {
    if location.is_file() {
        return Ok(location.canonicalize()?);
    }
    ensure!(
        location.is_dir(),
        "config path is neither a file nor a directory: {}",
        location.display()
    );
    let files = config_files(location)?;
    let host = first_host(args);
    let matches = matching_files(&files, host)?;
    if matches.len() == 1 {
        return Ok(matches[0].canonicalize()?);
    }
    ensure!(
        io::stdin().is_terminal(),
        "multiple configs match{}; use --config FILE",
        host.map_or(String::new(), |name| format!(" node {name:?}"))
    );
    eprintln!(
        "Choose a config{}:",
        host.map_or(String::new(), |name| format!(" for node {name:?}"))
    );
    for (index, path) in matches.iter().enumerate() {
        eprintln!("  {}. {}", index + 1, path.display());
    }
    eprint!("Config number [1-{}]: ", matches.len());
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    let index = answer
        .trim()
        .parse::<usize>()
        .context("enter a config number")?;
    if !(1..=matches.len()).contains(&index) {
        bail!("config number out of range");
    }
    matches[index - 1].canonicalize().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn write_config(dir: &Path, file: &str, node: &str) {
        fs::write(
            dir.join(file),
            format!("[nodes.{node}]\nhost = \"127.0.0.1\"\nuser = \"test\"\nagent = true\n"),
        )
        .unwrap();
    }

    #[test]
    fn first_host_comes_from_command_or_first_remote_endpoint() {
        let args = |values: &[&str]| {
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(first_host(&args(&["status", "db"])), Some("db"));
        assert_eq!(first_host(&args(&["transfers", "db"])), Some("db"));
        assert_eq!(
            first_host(&args(&["local.txt", "db:/tmp/test"])),
            Some("db")
        );
        assert_eq!(
            first_host(&args(&["edge:/tmp/test", "db:/tmp/test"])),
            Some("edge")
        );
        assert_eq!(first_host(&args(&["config", "check"])), None);
        assert_eq!(first_host(&args(&["status"])), None);
    }

    #[test]
    fn directory_selects_unique_host_and_detects_duplicates() {
        let dir = std::env::temp_dir().join(format!("mssh-configs-{}", Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        write_config(&dir, "a.toml", "edge");
        write_config(&dir, "b.toml", "db");
        let files = config_files(&dir).unwrap();
        assert_eq!(
            matching_files(&files, Some("db")).unwrap(),
            vec![dir.join("b.toml")]
        );
        assert_eq!(
            select_path(&dir, &["db".into()]).unwrap(),
            dir.join("b.toml").canonicalize().unwrap()
        );
        write_config(&dir, "c.toml", "db");
        let files = config_files(&dir).unwrap();
        assert_eq!(matching_files(&files, Some("db")).unwrap().len(), 2);
        assert!(matching_files(&files, Some("missing")).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn included_toml_is_not_offered_as_a_separate_config() {
        let dir = std::env::temp_dir().join(format!("mssh-configs-{}", Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("main.toml"), "include = [\"fragment.toml\"]\n").unwrap();
        write_config(&dir, "fragment.toml", "db");
        assert_eq!(config_files(&dir).unwrap(), vec![dir.join("main.toml")]);
        assert_eq!(
            select_path(&dir, &["db".into()]).unwrap(),
            dir.join("main.toml").canonicalize().unwrap()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn directory_parse_errors_do_not_echo_passwords() {
        let dir = std::env::temp_dir().join(format!("mssh-configs-{}", Uuid::new_v4()));
        fs::create_dir(&dir).unwrap();
        fs::write(
            dir.join("bad.toml"),
            "[nodes.edge]\npassword = \"private-test-password\" invalid\n",
        )
        .unwrap();
        let error = config_files(&dir).unwrap_err();
        assert!(!format!("{error:#}").contains("private-test-password"));
        fs::remove_dir_all(dir).unwrap();
    }
}
