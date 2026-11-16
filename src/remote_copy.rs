use crate::{config::Config, transfer};
use anyhow::{Context, Result, bail, ensure};
use russh_sftp::{
    client::SftpSession,
    protocol::{FileAttributes, FileType, OpenFlags},
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

fn child(parent: &str, name: &str) -> Result<String> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains('/')
            && !name.contains('\\'),
        "unsafe directory entry name: {name:?}"
    );
    Ok(format!("{}/{}", parent.trim_end_matches('/'), name))
}

fn copy_attributes(attrs: &FileAttributes) -> FileAttributes {
    FileAttributes {
        permissions: attrs.permissions.map(|mode| mode & 0o7777),
        atime: attrs.atime,
        mtime: attrs.mtime,
        ..Default::default()
    }
}

async fn copy_file(
    config: &Config,
    source_sftp: &SftpSession,
    destination_sftp: &SftpSession,
    source: &str,
    destination: &str,
    attrs: &FileAttributes,
) -> Result<()> {
    ensure!(
        !destination_sftp.try_exists(destination).await?,
        "destination already exists: {destination}"
    );
    let temporary = format!("{destination}.mssh-{}.part", Uuid::new_v4());
    let mut input = source_sftp.open(source).await?;
    let mut output = destination_sftp
        .open_with_flags_and_attributes(
            &temporary,
            OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
            FileAttributes {
                permissions: Some(0o600),
                ..Default::default()
            },
        )
        .await?;
    let result = async {
        let mut buffer = vec![0u8; config.settings.chunk_size.min(1024 * 1024)];
        let mut hash = Sha256::new();
        let mut written = 0u64;
        loop {
            let count = input.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            written += count as u64;
            ensure!(
                written <= attrs.len(),
                "source changed while copying: {source}"
            );
            output.write_all(&buffer[..count]).await?;
            if config.verify {
                hash.update(&buffer[..count]);
            }
        }
        output.close().await?;
        input.close().await?;
        let current = source_sftp.metadata(source).await?;
        ensure!(
            written == attrs.len() && current.len() == attrs.len() && current.mtime == attrs.mtime,
            "source changed while copying: {source}"
        );
        ensure!(
            destination_sftp.metadata(&temporary).await?.len() == written,
            "destination size differs after copying: {destination}"
        );
        if config.verify {
            let expected = hex::encode(hash.finalize());
            let actual = transfer::remote_hash_sftp(
                destination_sftp,
                &temporary,
                config.settings.chunk_size,
            )
            .await?;
            ensure!(actual == expected, "SHA-256 mismatch: {destination}");
        }
        destination_sftp
            .set_metadata(&temporary, copy_attributes(attrs))
            .await?;
        ensure!(
            !destination_sftp.try_exists(destination).await?,
            "destination already exists: {destination}"
        );
        destination_sftp.rename(&temporary, destination).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        let _ = destination_sftp.remove_file(&temporary).await;
    }
    result
}

pub async fn copy(
    config: &Config,
    source_host: &str,
    source: &str,
    destination_host: &str,
    destination: &str,
    recursive: bool,
) -> Result<()> {
    ensure!(
        source.starts_with('/') && destination.starts_with('/'),
        "remote paths must be absolute"
    );
    let source_connection = transfer::connect_any(config, source_host).await?;
    let source_sftp = transfer::sftp(&source_connection).await?;
    let destination_connection = transfer::connect_any(config, destination_host).await?;
    let destination_sftp = transfer::sftp(&destination_connection).await?;
    let attrs = source_sftp.symlink_metadata(source).await?;
    let name = source
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .context("invalid remote source filename")?;
    let destination = if destination_sftp.try_exists(destination).await?
        && destination_sftp
            .metadata(destination)
            .await?
            .file_type()
            .is_dir()
    {
        child(destination, name)?
    } else {
        destination.to_owned()
    };
    if source_host == destination_host {
        let source_resolved = source_sftp.canonicalize(source).await?;
        let (parent, leaf) = destination
            .rsplit_once('/')
            .context("invalid remote destination")?;
        let parent = if parent.is_empty() { "/" } else { parent };
        let destination_resolved = format!(
            "{}/{}",
            destination_sftp
                .canonicalize(parent)
                .await?
                .trim_end_matches('/'),
            leaf
        );
        ensure!(
            source_resolved != destination_resolved,
            "source and destination are the same file"
        );
        if attrs.file_type().is_dir() {
            ensure!(
                !destination_resolved
                    .starts_with(&format!("{}/", source_resolved.trim_end_matches('/'))),
                "cannot copy a directory into itself"
            );
        }
    }
    ensure!(
        !destination_sftp.try_exists(&destination).await?,
        "destination already exists: {destination}"
    );
    match attrs.file_type() {
        FileType::File => {
            copy_file(
                config,
                &source_sftp,
                &destination_sftp,
                source,
                &destination,
                &attrs,
            )
            .await
        }
        FileType::Dir => {
            ensure!(recursive, "source is a directory; use -r");
            destination_sftp.create_dir(&destination).await?;
            let mut stack = vec![(source.to_owned(), destination)];
            let mut directories = Vec::new();
            while let Some((from, to)) = stack.pop() {
                directories.push((to.clone(), source_sftp.metadata(&from).await?));
                for entry in source_sftp.read_dir(&from).await? {
                    let name = entry.file_name();
                    let from_child = child(&from, &name)?;
                    let to_child = child(&to, &name)?;
                    let attrs = source_sftp.symlink_metadata(&from_child).await?;
                    match attrs.file_type() {
                        FileType::File => {
                            copy_file(
                                config,
                                &source_sftp,
                                &destination_sftp,
                                &from_child,
                                &to_child,
                                &attrs,
                            )
                            .await?
                        }
                        FileType::Dir => {
                            destination_sftp.create_dir(&to_child).await?;
                            stack.push((from_child, to_child));
                        }
                        FileType::Symlink => {
                            let link = source_sftp.read_link(&from_child).await?;
                            destination_sftp.symlink(link, &to_child).await?;
                        }
                        _ => bail!("unsupported remote file type: {from_child}"),
                    }
                }
            }
            for (path, attrs) in directories.into_iter().rev() {
                destination_sftp
                    .set_metadata(path, copy_attributes(&attrs))
                    .await?;
            }
            Ok(())
        }
        _ => bail!("source is not a regular file or directory: {source}"),
    }
}
