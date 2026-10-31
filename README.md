# MultiHop SSH (`mssh`) prototype

Rust CLI for reaching a Linux host through explicitly configured SSH chains. Every hop uses a separate Russh connection inside the preceding hop's `direct-tcpip` channel. The client pins a SHA-256 host-key fingerprint for each logical node. Intermediate hosts need no extra software.

## Build

```sh
cargo build --release
```

Copy [the example](examples/config.toml) to `~/.config/mssh/config.toml` and replace every placeholder host-key fingerprint with one verified through a trusted channel. Passwords and key passphrases are read from named environment variables. On Linux, `agent = true` uses `SSH_AUTH_SOCK`.

```sh
mssh config check
mssh routes --target internal
mssh status --target internal --details
mssh status --watch --json
mssh status --cached
mssh doctor internal
mssh ssh internal
mssh put internal ./archive.tar /home/alice/archive.tar
mssh get internal /home/alice/archive.tar ./archive.tar
mssh transfer list
mssh transfer resume UUID
```

`mssh ssh` starts a fresh shell after a transport failure. Press Ctrl-] to force a reconnect. Exiting the remote shell normally exits the client. Remote processes and shell state are not preserved.

Transfers use unique `.mssh-UUID.part` files and owner-only local journals under `~/.local/state/mssh/transfers`. Blocks are checked after upload; full files are checked with SHA-256 before publication. Uploads use multiple routes concurrently and retry outstanding blocks on surviving routes. Publishing a remote file requires the server's `hardlink@openssh.com` extension, which prevents overwriting an existing name; a verified temporary file is retained when that extension is unavailable. Downloads use a local hard link for the same no-overwrite guarantee. `transfer resume` rechecks source identity and rewrites blocks before the final hash. `transfer cancel` removes the local journal and leaves a remote temporary file for manual cleanup.

## Current limits

This is a working prototype, not the v0.1 acceptance release. Directory recursion, detailed telemetry and route states, PTY resize propagation, and automated integration tests against multi-hop SSH/SFTP servers are pending. The client saves the last status result but not a history of measurements. Linux runtime behavior still needs testing with real servers. SSH host certificates are rejected until CA and principal validation can be configured.
