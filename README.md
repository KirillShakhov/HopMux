# MultiHop SSH (`mssh`)

`mssh` connects to Linux hosts through configured SSH chains and copies files over SFTP. Each hop has its own SSH connection; intermediate hosts need no additional software. See [LICENSE](LICENSE) for GPL-3.0-only terms.

## Build and configuration

```sh
cargo build --release
```

To build and test in one reusable Docker container:

```sh
docker compose up -d --build
docker compose exec -T dev cargo test --locked
docker compose exec -T dev cargo build --locked
```

The Cargo registry and build output use persistent Docker volumes. After the first setup, run further checks with `docker compose exec` without rebuilding the image.

Copy [the example](examples/config.toml) to `~/.config/mssh/config.toml`. Nodes can use a key, SSH agent, password, or a password environment variable. Set `host_key = "SHA256:..."` to pin a host key; unpinned nodes produce a warning. A node with no explicit route is reachable directly.

## Commands

```sh
mssh edge_a
mssh edge_a:/home/niias/test.yml ./downloads/
mssh ./download/test.yml edge_a:/home/niias
mssh -r ./project edge_a:/home/niias
mssh -d edge_a:/home/niias/test.yml ./downloads/
mssh transfers edge_a
mssh status edge_a --details
mssh --debug status edge_a
mssh --debug edge_a:/home/niias/test.yml ./downloads/
mssh --chunk-size auto --min-chunk-size 256KiB --max-chunk-size 8MiB edge_a:/data/big.bin ./downloads/
mssh --route-chunk slow=512KiB --chunk-timeout 60 ./big.bin edge_a:/data/
mssh status --json
mssh config check
```

Copying to an existing directory appends the source name, as with `scp`. Remote paths must be absolute. Use `-r` for directories. All available routes are used for file transfers by default; `--route NAME` selects a route and can be repeated. `--max-routes N` limits the number used concurrently.

File transfers adapt batch size separately for each route. The initial size is `settings.chunk_size` (1 MiB by default); after three verified batches at a size, the worker compares median throughput and may double it. It stops increasing when a larger size is slower, and shrinks after two consecutive failed batches. The first sample on a route includes SSH/SFTP connection time; each sample includes local I/O, remote readback for uploads, and remote journal persistence. A single failure does not shrink the batch. Short files may finish before there are enough samples to tune.

`--chunk-size auto` selects adaptation explicitly; `--chunk-size SIZE` fixes the size. `--min-chunk-size SIZE` and `--max-chunk-size SIZE` set the auto range (defaults: 256 KiB and 8 MiB). Repeat `--route-chunk ROUTE=SIZE` to fix individual routes at different sizes. `--chunk-timeout SECONDS` limits the I/O for one batch (default: 120 seconds, allowed: 1–300). Sizes accept bytes, `KiB`, or `MiB`, must be multiples of 64 KiB, and may be 64 KiB–32 MiB. Total transfer buffers are limited to 256 MiB. Completed 256 KiB or smaller atoms remain in the remote journal, so changing batch sizes does not discard verified progress. Older journals retain their original fixed block size when resumed.

`--debug` works with every command and writes timestamped operation logs to stderr, including route selection, SSH authentication method, SFTP activity, block offsets, verification, and remote journal updates. It does not print passwords or file contents. `--details` only adds information to the `status` report. With `-d --debug`, the worker keeps writing to the current terminal's stderr while that terminal remains open; no client log file is created.

An interrupted file copy leaves a partial file and a journal under `~/.mssh/transfers` on the destination SSH server. For downloads, the journal is on the source SSH server and the local `.part` contains only downloaded file data. Repeat the same copy command to choose whether to continue or restart. A second worker is rejected while the first is active. For noninteractive use, pass `--continue-partial` or `--restart`. `-d` starts a detached transfer worker; it also accepts those two options when a partial transfer already exists. `mssh transfers TARGET` reads the remote journals. There is no local transfer journal or status cache.

Uploads verify each written chunk and the final SHA-256 before publishing. Downloads verify the final SHA-256. Publishing does not overwrite an existing destination. Recursive copies preserve regular files, empty directories, symbolic links, permissions, and modification times on Linux.

`status` probes routes and displays connection times, SSH round-trip times, hop timings with `--details`, and transfer progress from remote journals. `status --details` and `transfers TARGET` show the current batch size and route timing. `config check` validates the configuration. `mssh TARGET` opens an SSH shell and reconnects after a transport failure; Ctrl-] forces a reconnect.

## Limits

Background transfers need the client computer to stay running. A partial download can be continued only where its local `.part` file is available; on another computer it must be restarted. Remote state requires write access to the SSH account's home directory. Recursive directory copies cannot yet continue as one operation after an interruption; individual files have remote journals. SSH host certificates are rejected until CA and principal validation are configured.

## Local integration stand

On Ubuntu WSL, run `tests/local-stand.sh` as root with the ordinary Linux username as its argument. The script starts three loopback SSH servers on ports 37221–37223 and writes `/tmp/hopmux-stand/config.toml`. Run `tests/local-stand.sh stop` to stop them.
