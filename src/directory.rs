use crate::{config::Config, transfer};
use anyhow::{Context, Result, bail, ensure};
use russh_sftp::{
    client::SftpSession,
    protocol::{FileAttributes, FileType},
};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn remote_child(parent: &str, name: &str) -> String {
    format!("{}/{}", parent.trim_end_matches('/'), name)
}

fn safe_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains('/')
            && !name.contains('\\'),
        "unsafe directory entry name: {name:?}"
    );
    Ok(())
}

fn local_name(path: &Path) -> Result<&str> {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .context("filename is not UTF-8")?;
    safe_name(name)?;
    Ok(name)
}

#[cfg(unix)]
fn local_attributes(metadata: &fs::Metadata) -> FileAttributes {
    use std::os::unix::fs::PermissionsExt;
    use std::time::UNIX_EPOCH;
    FileAttributes {
        permissions: Some(metadata.permissions().mode() & 0o7777),
        mtime: metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .and_then(|d| u32::try_from(d.as_secs()).ok()),
        atime: metadata
            .accessed()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .and_then(|d| u32::try_from(d.as_secs()).ok()),
        ..Default::default()
    }
}

#[cfg(not(unix))]
fn local_attributes(_: &fs::Metadata) -> FileAttributes {
    FileAttributes::default()
}

async fn set_remote_metadata(
    sftp: &SftpSession,
    path: &str,
    metadata: &fs::Metadata,
) -> Result<()> {
    let attrs = local_attributes(metadata);
    if attrs.permissions.is_some() || attrs.mtime.is_some() {
        crate::debug_log!("directory: setting remote metadata on {path}");
        sftp.set_metadata(path, attrs).await?;
    }
    Ok(())
}

fn set_local_metadata(path: &Path, attrs: &FileAttributes) -> Result<()> {
    crate::debug_log!("directory: setting local metadata on {}", path.display());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(mode) = attrs.permissions {
            fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o7777))?;
        }
    }
    if let Some(mtime) = attrs.mtime {
        filetime::set_file_mtime(path, filetime::FileTime::from_unix_time(mtime as i64, 0))?;
    }
    Ok(())
}

pub async fn put_directory(
    config: &Config,
    target: &str,
    source: &Path,
    destination: &str,
) -> Result<()> {
    crate::debug_log!("directory upload: local:{} -> {}:{}", source.display(), target, destination);
    ensure!(source.is_dir(), "local source is not a directory");
    ensure!(
        destination.starts_with('/'),
        "remote destination must be an absolute path"
    );
    let connection = transfer::connect_any(config, target).await?;
    let sftp = transfer::sftp(&connection).await?;
    ensure!(
        !sftp.try_exists(destination).await?,
        "remote destination already exists"
    );
    sftp.create_dir(destination).await?;
    crate::debug_log!("directory upload: created remote directory {destination}");
    let mut stack = vec![(source.to_path_buf(), destination.to_owned())];
    let mut directories = Vec::new();
    while let Some((local_dir, remote_dir)) = stack.pop() {
        directories.push((remote_dir.clone(), fs::metadata(&local_dir)?));
        for entry in fs::read_dir(&local_dir)? {
            let entry = entry?;
            let local = entry.path();
            let name = local_name(&local)?;
            let remote = remote_child(&remote_dir, name);
            let metadata = fs::symlink_metadata(&local)?;
            if metadata.file_type().is_symlink() {
                crate::debug_log!("directory upload: symlink local:{} -> {}:{remote}", local.display(), target);
                #[cfg(unix)]
                {
                    let link = fs::read_link(&local)?;
                    let link = link.to_str().context("symlink target is not UTF-8")?;
                    sftp.symlink(link, &remote).await?;
                }
                #[cfg(not(unix))]
                bail!("symbolic link transfer requires Linux");
            } else if metadata.is_dir() {
                crate::debug_log!("directory upload: creating remote directory {remote}");
                sftp.create_dir(&remote).await?;
                stack.push((local, remote));
            } else if metadata.is_file() {
                crate::debug_log!("directory upload: file local:{} -> {}:{remote}", local.display(), target);
                transfer::put(config, target, &local, &remote).await?;
                set_remote_metadata(&sftp, &remote, &metadata).await?;
            } else {
                bail!("unsupported local file type: {}", local.display());
            }
        }
    }
    for (remote, metadata) in directories.into_iter().rev() {
        set_remote_metadata(&sftp, &remote, &metadata).await?;
    }
    crate::debug_log!("directory upload: completed {destination}");
    Ok(())
}

pub async fn get_directory(
    config: &Config,
    target: &str,
    source: &str,
    destination: &Path,
) -> Result<()> {
    crate::debug_log!("directory download: {}:{} -> local:{}", target, source, destination.display());
    ensure!(
        source.starts_with('/'),
        "remote source must be an absolute path"
    );
    ensure!(!destination.exists(), "local destination already exists");
    let connection = transfer::connect_any(config, target).await?;
    let sftp = transfer::sftp(&connection).await?;
    let source_meta = sftp.metadata(source).await?;
    ensure!(
        source_meta.file_type().is_dir(),
        "remote source is not a directory"
    );
    fs::create_dir(destination)?;
    crate::debug_log!("directory download: created local directory {}", destination.display());
    let mut stack = vec![(source.to_owned(), destination.to_path_buf())];
    let mut directories: Vec<(PathBuf, FileAttributes)> = Vec::new();
    while let Some((remote_dir, local_dir)) = stack.pop() {
        let attrs = sftp.metadata(&remote_dir).await?;
        directories.push((local_dir.clone(), attrs));
        for entry in sftp.read_dir(&remote_dir).await? {
            let name = entry.file_name();
            safe_name(&name)?;
            let remote = remote_child(&remote_dir, &name);
            let local = local_dir.join(&name);
            let attrs = sftp.symlink_metadata(&remote).await?;
            match attrs.file_type() {
                FileType::Dir => {
                    crate::debug_log!("directory download: creating local directory {}", local.display());
                    fs::create_dir(&local)?;
                    stack.push((remote, local));
                }
                FileType::File => {
                    crate::debug_log!("directory download: file {}:{remote} -> local:{}", target, local.display());
                    transfer::get(config, target, &remote, &local).await?;
                    set_local_metadata(&local, &attrs)?;
                }
                FileType::Symlink => {
                    crate::debug_log!("directory download: symlink {}:{remote} -> local:{}", target, local.display());
                    #[cfg(unix)]
                    {
                        let link = sftp.read_link(&remote).await?;
                        std::os::unix::fs::symlink(link, &local)?;
                    }
                    #[cfg(not(unix))]
                    bail!("symbolic link transfer requires Linux");
                }
                _ => bail!("unsupported remote file type: {remote}"),
            }
        }
    }
    for (local, attrs) in directories.into_iter().rev() {
        set_local_metadata(&local, &attrs)?;
    }
    crate::debug_log!("directory download: completed {}", destination.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_remote_path_escape() {
        for name in ["..", "../outside", "sub/file", "sub\\file", ""] {
            assert!(safe_name(name).is_err());
        }
        assert!(safe_name("ordinary.txt").is_ok());
    }
}
