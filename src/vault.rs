use anyhow::{Context, Result, bail, ensure};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::OnceCell;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const VAULT_NAME: &str = "vault.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Passwords {
    pub passwords: BTreeMap<String, String>,
}

impl Drop for Passwords {
    fn drop(&mut self) {
        for password in self.passwords.values_mut() {
            password.zeroize();
        }
    }
}

#[derive(Clone, Default)]
pub struct VaultStore {
    path: PathBuf,
    loaded: Arc<OnceCell<Result<Passwords, String>>>,
}

impl std::fmt::Debug for VaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultStore")
            .field("path", &self.path)
            .field("loaded", &self.loaded.get().is_some())
            .finish()
    }
}

impl VaultStore {
    pub fn at(path: PathBuf) -> Self {
        Self {
            path,
            loaded: Arc::new(OnceCell::new()),
        }
    }

    pub async fn get(&self, name: &str) -> Result<String> {
        let path = self.path.clone();
        let result = self
            .loaded
            .get_or_init(|| async move {
                let result = tokio::task::spawn_blocking(move || {
                    let passphrase = Zeroizing::new(rpassword::prompt_password(format!(
                        "Vault password ({}): ",
                        path.display()
                    ))?);
                    let bytes = fs::read(&path)
                        .with_context(|| format!("cannot read vault {}", path.display()))?;
                    decrypt(&bytes, &passphrase)
                })
                .await;
                match result {
                    Ok(Ok(passwords)) => Ok(passwords),
                    Ok(Err(error)) => Err(format!("{error:#}")),
                    Err(error) => Err(format!("vault unlock task failed: {error}")),
                }
            })
            .await;
        let passwords = result
            .as_ref()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        passwords
            .passwords
            .get(name)
            .cloned()
            .with_context(|| format!("password_vault entry {name:?} is missing"))
    }

    pub fn snapshot(&self) -> Result<Option<Vec<u8>>> {
        self.loaded
            .get()
            .and_then(|result| result.as_ref().ok())
            .map(serde_json::to_vec)
            .transpose()
            .map_err(Into::into)
    }

    pub fn load_snapshot(&self, bytes: &[u8]) -> Result<()> {
        let passwords: Passwords = serde_json::from_slice(bytes)?;
        validate(&passwords)?;
        self.loaded
            .set(Ok(passwords))
            .map_err(|_| anyhow::anyhow!("vault already loaded"))
    }
}

pub fn path(config_path: &Path, explicit: Option<&Path>, for_write: bool) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    let nearby = config_path
        .parent()
        .context("config path has no parent")?
        .join(VAULT_NAME);
    if for_write || nearby.exists() {
        return Ok(nearby);
    }
    Ok(crate::config::default_path()?.join(VAULT_NAME))
}

fn validate(passwords: &Passwords) -> Result<()> {
    for (name, password) in &passwords.passwords {
        ensure!(
            !name.is_empty() && !password.is_empty(),
            "vault names and passwords must not be empty"
        );
    }
    Ok(())
}

fn key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32]> {
    ensure!(!passphrase.is_empty(), "vault password must not be empty");
    let mut key = [0u8; 32];
    let params = Params::new(64 * 1024, 3, 1, None)
        .map_err(|error| anyhow::anyhow!("invalid vault KDF parameters: {error}"))?;
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|error| anyhow::anyhow!("cannot derive vault key: {error}"))?;
    Ok(key)
}

pub fn encrypt(passwords: &Passwords, passphrase: &str) -> Result<Vec<u8>> {
    validate(passwords)?;
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);
    let key = Zeroizing::new(key(passphrase, &salt)?);
    let cipher = XChaCha20Poly1305::new_from_slice(&*key)?;
    let plaintext = Zeroizing::new(toml::to_string(passwords)?);
    let ciphertext = cipher
        .encrypt(XNonce::from_slice(&nonce), plaintext.as_bytes())
        .map_err(|_| anyhow::anyhow!("vault encryption failed"))?;
    Ok(serde_json::to_vec_pretty(&Envelope {
        version: 1,
        salt: hex::encode(salt),
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(ciphertext),
    })?)
}

pub fn decrypt(bytes: &[u8], passphrase: &str) -> Result<Passwords> {
    let envelope: Envelope = serde_json::from_slice(bytes).context("invalid vault file")?;
    ensure!(envelope.version == 1, "unsupported vault version");
    let salt = hex::decode(envelope.salt).context("invalid vault salt")?;
    let nonce = hex::decode(envelope.nonce).context("invalid vault nonce")?;
    ensure!(
        salt.len() == 16 && nonce.len() == 24,
        "invalid vault parameters"
    );
    let ciphertext = hex::decode(envelope.ciphertext).context("invalid vault ciphertext")?;
    let key = Zeroizing::new(key(passphrase, &salt)?);
    let cipher = XChaCha20Poly1305::new_from_slice(&*key)?;
    let plaintext = Zeroizing::new(
        cipher
            .decrypt(XNonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| anyhow::anyhow!("wrong vault password or damaged vault"))?,
    );
    let passwords: Passwords = toml::from_str(std::str::from_utf8(&plaintext)?)?;
    validate(&passwords)?;
    Ok(passwords)
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    ensure!(
        !path.exists(),
        "destination already exists: {}",
        path.display()
    );
    let temporary = path.with_file_name(format!(".mssh-{}.tmp", Uuid::new_v4()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        ensure!(
            !path.exists(),
            "destination already exists: {}",
            path.display()
        );
        fs::hard_link(&temporary, path)?;
        Ok::<(), anyhow::Error>(())
    })();
    let _ = fs::remove_file(&temporary);
    result
}

pub fn command(config_path: &Path, explicit: Option<&Path>, args: &[String]) -> Result<()> {
    match args {
        [action, input] if action == "encrypt" => {
            let input = Path::new(input);
            let decoded = Zeroizing::new(
                fs::read_to_string(input)
                    .with_context(|| format!("cannot read {}", input.display()))?,
            );
            let passwords: Passwords = toml::from_str(&decoded)?;
            let vault_path = path(config_path, explicit, true)?;
            let first = Zeroizing::new(rpassword::prompt_password("New vault password: ")?);
            let second = Zeroizing::new(rpassword::prompt_password("Confirm vault password: ")?);
            ensure!(first == second, "vault passwords do not match");
            let encrypted = encrypt(&passwords, &first)?;
            write_private(&vault_path, &encrypted)?;
            println!("vault written: {}", vault_path.display());
            Ok(())
        }
        [action, output] if action == "decrypt" => {
            let vault_path = path(config_path, explicit, false)?;
            let password = Zeroizing::new(rpassword::prompt_password("Vault password: ")?);
            let plaintext = decrypt(
                &fs::read(&vault_path)
                    .with_context(|| format!("cannot read vault {}", vault_path.display()))?,
                &password,
            )?;
            let output = Path::new(output);
            let decoded = Zeroizing::new(toml::to_string_pretty(&plaintext)?);
            write_private(output, decoded.as_bytes())?;
            println!("vault decrypted: {}", output.display());
            Ok(())
        }
        _ => bail!(
            "usage: mssh [--vault-file FILE] vault encrypt PASSWORDS.toml | vault decrypt OUTPUT.toml"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_vault_round_trip_and_wrong_password() {
        let passwords = Passwords {
            passwords: BTreeMap::from([
                ("server1".into(), "secret with spaces".into()),
                ("server2".into(), "другой пароль".into()),
            ]),
        };
        let encrypted = encrypt(&passwords, "master password").unwrap();
        assert!(!String::from_utf8_lossy(&encrypted).contains("secret with spaces"));
        assert_eq!(
            decrypt(&encrypted, "master password").unwrap().passwords,
            passwords.passwords
        );
        assert!(decrypt(&encrypted, "wrong").is_err());
    }

    #[tokio::test]
    async fn worker_snapshot_supplies_password_without_prompt() {
        let store = VaultStore::default();
        let passwords = Passwords {
            passwords: BTreeMap::from([("shared".into(), "secret".into())]),
        };
        store
            .load_snapshot(&serde_json::to_vec(&passwords).unwrap())
            .unwrap();
        assert_eq!(store.get("shared").await.unwrap(), "secret");
        assert!(store.get("missing").await.is_err());
        assert!(!format!("{store:?}").contains("secret"));
    }

    #[test]
    fn vault_path_prefers_active_config_then_default() {
        let base = std::env::temp_dir().join(format!("mssh-vault-test-{}", Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        let config = base.join("config.toml");
        let nearby = base.join(VAULT_NAME);
        assert_eq!(path(&config, None, true).unwrap(), nearby);
        assert_eq!(
            path(&config, None, false).unwrap(),
            crate::config::default_path().unwrap().join(VAULT_NAME)
        );
        fs::write(&nearby, b"test").unwrap();
        assert_eq!(path(&config, None, false).unwrap(), nearby);
        let explicit = base.join("custom.vault");
        assert_eq!(path(&config, Some(&explicit), false).unwrap(), explicit);
        fs::remove_dir_all(base).unwrap();
    }
}
