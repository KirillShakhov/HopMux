# MultiHop SSH (`mssh`)

`mssh` connects to Linux hosts through configured SSH chains and copies files over SFTP. Each hop has its own SSH connection; intermediate hosts need no additional software. See [LICENSE](LICENSE) for GPL-3.0-only terms.

## Build and configuration

```sh
cargo build --release
```

To build and test in one reusable Docker container:

```sh
docker compose -f compose.dev.yaml up -d --build
docker compose -f compose.dev.yaml exec -T dev cargo test --locked
docker compose -f compose.dev.yaml exec -T dev cargo build --locked
```

The Cargo registry and build output use persistent Docker volumes. After the first setup, run further checks with `docker compose -f compose.dev.yaml exec` without rebuilding the image.

Copy [the example](examples/config.toml) to `~/.config/mssh/config.toml`. Nodes can use a key, SSH agent, password, or a password environment variable. Set `host_key = "SHA256:..."` to pin a host key; unpinned nodes produce a warning. List only hosts reachable from the client in `direct_hosts`. List directed path fragments in `paths`, with the highest-priority fragments first:

```toml
direct_hosts = ["edge_a", "edge_b"]
paths = [
  ["edge_a", "relay"],
  ["relay", "internal"],
  ["edge_b", "internal"],
]
```

`mssh` joins fragments at shared hosts, so the example yields `local -> edge_a -> relay -> internal` before `local -> edge_b -> internal`. A long fragment such as `["edge_a", "relay", "internal"]` also yields routes to `relay` and `internal`. Every prefix is usable; paths are directed and never imply a reverse connection. Generated route IDs are target-based (`internal-1`, `internal-2`) and are shown by `status`. Existing `[[routes]]` configurations still load, but cannot be mixed with `paths` or `direct_hosts`.

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
mssh --verify ./important.bin edge_a:/data/
mssh --chunk-size auto --min-chunk-size 256KiB --max-chunk-size 8MiB edge_a:/data/big.bin ./downloads/
mssh --route-chunk internal-2=512KiB --chunk-timeout 60 ./big.bin internal:/data/
mssh status --json
mssh config check
mssh serve
```

Copying to an existing directory appends the source name, as with `scp`. Remote paths must be absolute. Use `-r` for directories. All available routes are used for file transfers by default; `--route ID` selects a generated route ID such as `internal-1` and can be repeated. `--max-routes N` limits the number used concurrently.

File transfers adapt batch size separately for each route. The initial size is `settings.chunk_size` (1 MiB by default); after three completed batches at a size, the worker compares median throughput and may double it. It stops increasing when a larger size is slower or would likely exceed 80% of the batch timeout, and shrinks after two consecutive failed batches. The first successful sample on a route includes SSH/SFTP connection time; failed connections count as unsuccessful samples. Each successful sample includes local I/O and SSH/SFTP transfer time. Remote journal checkpoints are serialized without holding the shared transfer-state lock. A single failure does not shrink the batch. Short files may finish before there are enough samples to tune.

`--chunk-size auto` selects adaptation explicitly; `--chunk-size SIZE` fixes the size. `--min-chunk-size SIZE` and `--max-chunk-size SIZE` set the auto range (defaults: 256 KiB and 8 MiB). Repeat `--route-chunk ROUTE=SIZE` to fix individual routes at different sizes. `--chunk-timeout SECONDS` limits the I/O for one batch (default: 120 seconds, allowed: 1–300). Sizes accept bytes, `KiB`, or `MiB`, must be multiples of 64 KiB, and may be 64 KiB–32 MiB. Total transfer buffers are limited to 256 MiB. Completed 256 KiB or smaller atoms remain in the remote journal, so changing batch sizes does not discard verified progress. Older journals retain their original fixed block size when resumed.

`--debug` works with every command and writes timestamped operation logs to stderr, including route selection, SSH authentication method, SFTP activity, block offsets, verification, and remote journal updates. It does not print passwords or file contents. `--details` only adds information to the `status` report. With `-d --debug`, the worker keeps writing to the current terminal's stderr while that terminal remains open; no client log file is created.

An interrupted file copy leaves a partial file and a journal under `~/.mssh/transfers` on the destination SSH server. For downloads, the journal is on the source SSH server and the local `.part` contains only downloaded file data. Progress is checkpointed every 16 MiB or 30 seconds and at worker completion; after an abrupt interruption, data since the last checkpoint may be transferred again. Repeat the same copy command to choose whether to continue or restart. A second worker is rejected while the first is active. For noninteractive use, pass `--continue-partial` or `--restart`. `-d` starts a detached transfer worker; it also accepts those two options when a partial transfer already exists. `mssh transfers TARGET` reads the remote journals. There is no local transfer journal or status cache.

File copies prioritize speed: uploads do not read back each chunk, and SHA-256 verification is off by default. `--verify` enables one final SHA-256 check before publishing. For uploads, the source is hashed before transfer and the remote part is hashed at the end; for downloads, the remote source is hashed before transfer and the local result afterward. On Linux servers, the remote hash is computed on the server so the file does not cross one slow route again; SFTP reading is used when the server command is unavailable. The verification choice is stored in the remote journal and retained on resume. Without `--verify`, size and modification time are checked for source changes, and SSH/SFTP acknowledgments establish completion. Publishing does not overwrite an existing destination. Recursive copies preserve regular files, empty directories, symbolic links, permissions, and modification times on Linux.

`status` probes routes and displays connection times, SSH round-trip times, hop timings with `--details`, and transfer progress from remote journals. `status --details` and `transfers TARGET` show the current batch size and route timing. `config check` validates the configuration.

`mssh TARGET` opens an SSH shell. It automatically uses `tmux` on the target when available, otherwise `screen`, otherwise a plain shell. `mssh` only checks for these programs; it never installs packages on the target. With `tmux` or `screen`, a route change reconnects to the same remote session, preserving running programs such as `nano`. A plain shell reconnects after a transport failure, but remote processes tied to the broken SSH channel cannot be preserved. Ctrl-] forces a reconnect.

While a `tmux` or `screen` shell is open, `mssh` checks SSH round-trip time and the rate of a small echo probe on a separate channel. Two poor checks trigger a search through the other routes. It chooses the first route meeting the interactive thresholds, or a substantially better route if every route is slow; a slow route is still usable. The default thresholds are 2 seconds and 2048 bytes/s, checked every 30 seconds. `--shell-max-rtt-ms N`, `--shell-min-rate N` (bytes/s), and `--shell-check-interval N` (seconds, `0` to disable checks) adjust this behavior. `--shell-mode auto|tmux|screen|plain` overrides mode detection; `auto` is the default. `--debug` logs probe results and route decisions.

## Local SSH and service ports

Add `local_ssh_port` to a node to expose its SSH port on the client's loopback address. The node's existing `port` remains the SSH port on the remote host. Use `[forwards.NAME]` for other TCP services:

```toml
[nodes.internal]
host = "10.0.0.15"
port = 22
user = "alice"
key_file = "~/.ssh/id_ed25519"
local_ssh_port = 2222

[forwards.postgres]
via = "internal"
listen = "127.0.0.1:15432"
remote = "127.0.0.1:5432"

[forwards.metrics]
via = "internal"
listen = "127.0.0.1:19090"
remote = "10.0.0.20:9090"
```

`via` selects the SSH node at the end of a configured path; `remote` is resolved from that node. `mssh serve` listens on every configured port in the foreground. For example, `ssh -p 2222 alice@127.0.0.1` reaches `internal`. Service forwards use SSH TCP forwarding on the final node; SSH-port bindings use the preceding hop to reach the final node's SSH port. No software is installed on remote hosts. `config check` rejects duplicate or overlapping local bindings and destinations without a route. Loopback is the only address used by `local_ssh_port`; a service `listen` address is explicit and may expose that service to other machines.

The listeners remain open when routes fail. `mssh` checks the active route every five seconds, tries other configured routes in priority order, and retries with a one-to-30-second backoff when all are unavailable. A newly accepted client waits up to 45 seconds for a route. Already established TCP streams cannot migrate to another SSH route and will end if their route breaks; new streams use the recovered route.

For automatic startup on Linux, install the binary and the included user service:

```sh
install -Dm755 target/release/mssh ~/.local/bin/mssh
install -Dm644 packaging/systemd/mssh-forwards.service ~/.config/systemd/user/mssh-forwards.service
systemctl --user daemon-reload
systemctl --user enable --now mssh-forwards.service
journalctl --user -u mssh-forwards.service -f
```

The user service runs `mssh serve` and restarts it after a process failure. It must be able to authenticate without a terminal; make the configured key, agent, or password environment available to the user service. To keep it running after logout and start it at boot before login, enable user lingering with `loginctl enable-linger "$USER"` where permitted. The service needs no network-online dependency because it binds local ports immediately and reconnects in the background.

Human-readable status, SSH, transfer, progress, and debug messages show routes as chains, for example `target-5: local -> entry5 -> relay5 -> relay5b -> hub -> target`. With `--details`, each hop appears as `relay5 -> relay5b: connect=...ms`. JSON output keeps separate route and hop fields.

## Limits

Background transfers need the client computer to stay running. A partial download can be continued only where its local `.part` file is available; on another computer it must be restarted. Remote state requires write access to the SSH account's home directory. Recursive directory copies cannot yet continue as one operation after an interruption; individual files have remote journals. SSH host certificates are rejected until CA and principal validation are configured.

## Docker network test stand

`compose.test.yaml` builds a separate SSH test network. The client's generated `/data/config.toml` lists five direct entry hosts and short path fragments, which compose into five routes to `target`. Routes 1–3 each have one relay; routes 4–5 each have two. All routes then pass through `hub` to `target`. Each route has two LTE bottlenecks at the same per-route rate: the client-to-entry link and the link after the second SSH hop on routes 1–3 or after the third SSH hop on routes 4–5. Other links are shaped to 1 Gbit/s. Each adjacent pair has its own Docker network, apart from the shared client-to-entry front network. The client cannot directly resolve or reach `target`. SSH host keys are generated for the stand and pinned in the client's generated config.

| Route | Both LTE links, each direction | Additional relays | Link RTTs, from client to target |
| --- | ---: | --- | --- |
| `target-1` | 0.5 Mbit/s | `relay1` | 100, 150, 200, 200 ms |
| `target-2` | 0.75 Mbit/s | `relay2` | 125, 200, 250, 200 ms |
| `target-3` | 1.0 Mbit/s | `relay3` | 150, 250, 300, 200 ms |
| `target-4` | 1.25 Mbit/s | `relay4`, `relay4b` | 175, 300, 100, 200, 200 ms |
| `target-5` | 1.5 Mbit/s | `relay5`, `relay5b` | 200, 100, 150, 250, 200 ms |

The RTT values are configured round trips for an individual link; actual `ping` results include Docker scheduling overhead. `relay3` to `hub` has 0.1% background loss and a four-second 3% loss burst every 30 seconds.

```sh
docker compose -f compose.test.yaml up -d --build --wait --wait-timeout 900
python3 tests/compose-stand/check.py
bash tests/compose-stand/forward.sh
docker compose -f compose.test.yaml exec -T client mssh --config /data/config.toml status target --details
docker compose -f compose.test.yaml exec -T client sh -c 'dd if=/dev/urandom of=/data/sample.bin bs=1M count=16 status=none && mssh --config /data/config.toml /data/sample.bin target:/home/mssh/data/sample.bin'
docker compose -f compose.test.yaml exec -T client sha256sum /data/sample.bin
docker compose -f compose.test.yaml exec -T target sha256sum /home/mssh/data/sample.bin
docker compose -f compose.test.yaml down
```

For another copy, choose a new destination filename; `mssh` does not overwrite the previous result. The stand uses named volumes for its keys, client test data, build cache, and target's home directory. Use `docker compose -f compose.test.yaml down -v` to remove them, including transfer journals. The older `tests/local-stand.sh` remains available for Ubuntu WSL; run it as root with the ordinary Linux username, and use `tests/local-stand.sh stop` to stop it.

The forward test builds the current client, checks an SSH-port binding and a service binding through five SSH hops, stops one route, then stops both selected routes and restores one. It verifies that the local listeners survive and new connections recover. It restores the stopped test nodes and ends the test forwarder when finished.
