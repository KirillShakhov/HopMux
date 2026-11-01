# MultiHop SSH (`mssh`) prototype

Rust CLI for reaching a Linux host through explicitly configured SSH chains. Every hop uses a separate Russh connection inside the preceding hop's `direct-tcpip` channel. The client pins a SHA-256 host-key fingerprint for each logical node. Intermediate hosts need no extra software.

## License

This project is licensed under the GNU General Public License version 3.0 only (`GPL-3.0-only`). See [LICENSE](LICENSE) for the full text.

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
mssh auth --check
mssh ssh internal
mssh put internal ./archive.tar /home/alice/archive.tar
mssh get internal /home/alice/archive.tar ./archive.tar
mssh put -r internal ./project /home/alice/project
mssh get -r internal /home/alice/project ./project
mssh transfer list
mssh transfer resume UUID
mssh transfer cancel UUID
```

`mssh ssh` starts a fresh shell after a transport failure. Press Ctrl-] to force a reconnect. Exiting the remote shell normally exits the client. Remote processes and shell state are not preserved.

Transfers use unique `.mssh-UUID.part` files and owner-only local journals under `~/.local/state/mssh/transfers`. Blocks are checked after upload; full files are checked with SHA-256 before publication. Uploads and downloads use multiple routes concurrently and retry outstanding blocks on surviving routes. Publishing a remote file requires the server's `hardlink@openssh.com` extension, which prevents overwriting an existing name; a verified temporary file is retained when that extension is unavailable. Downloads use a local hard link for the same no-overwrite guarantee. `transfer resume` rechecks source identity and rewrites blocks before the final hash. `transfer cancel` removes an interrupted transfer's temporary file and journal after checking that the transfer is no longer running and the destination has not appeared. Recursive transfers preserve regular files, empty directories, symbolic links, permissions, and modification times on Linux. A failed directory transfer may leave a partial destination tree; file journals can be resumed individually.

## Current limits

This is a working prototype, not the v0.1 acceptance release. `status` reports route RTT, per-hop connection times, transfer rates and byte counts, and probe and transfer error counters. Transfer telemetry is retained in journals and disappears when a journal is deleted; the client saves the last status result but not a history of measurements. Route states and automated integration tests against separate multi-hop SSH/SFTP servers are still incomplete. SSH host certificates are rejected until CA and principal validation can be configured.

## Local integration stand

On Ubuntu WSL, run `tests/local-stand.sh` as root with the ordinary Linux username as its argument. The script downloads `openssh-server` and `libwrap0` into `/tmp/hopmux-stand`, creates an `sshd` privilege-separation account if necessary, and starts three loopback SSH servers on ports 37221–37223. It does not install the packages system-wide. It writes a pinned test configuration to `/tmp/hopmux-stand/config.toml` and a passwordless key for this disposable stand. Run `tests/local-stand.sh stop` as root to stop the three servers.

```sh
cargo run -- --config /tmp/hopmux-stand/config.toml status --routes --details
cargo run -- --config /tmp/hopmux-stand/config.toml ssh target
```

The stand has been used to check three routes, multi-route upload and download, recursive transfers with an empty directory and symlink, resume after forced process termination, failover after an entrance stops, normal shell exit, manual and automatic terminal reconnection, cancellation of interrupted transfers, and a write failure that left the destination unpublished. Separate physical networks and modem failures remain untested.
