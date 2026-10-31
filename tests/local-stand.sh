#!/usr/bin/env bash
# Run as root inside Ubuntu WSL: bash tests/local-stand.sh kirill
set -euo pipefail

if (( EUID != 0 )); then
  echo "run this script as root" >&2
  exit 1
fi

stand_dir="/tmp/hopmux-stand"
package_dir="$stand_dir/package"
sshd_binary="$package_dir/usr/sbin/sshd"

is_running() {
  local pid_file="$1"
  [[ -s "$pid_file" ]] || return 1
  local pid
  pid="$(cat "$pid_file")"
  [[ "$(readlink "/proc/$pid/exe" 2>/dev/null || true)" == "$sshd_binary" ]]
}

if [[ "${1:-}" == "stop" ]]; then
  for name in edge_a edge_b target; do
    pid_file="$stand_dir/$name.pid"
    if is_running "$pid_file"; then
      kill "$(cat "$pid_file")"
    fi
  done
  echo "Stand stopped"
  exit 0
fi

stand_user="${1:?pass the unprivileged Linux username}"
mkdir -p "$stand_dir" "$package_dir" /run/sshd
chmod 711 "$stand_dir"
chmod 755 /run/sshd
if ! getent passwd sshd >/dev/null; then
  useradd --system --no-create-home --home-dir /run/sshd --shell /usr/sbin/nologin sshd
fi

if [[ ! -x "$sshd_binary" ]]; then
  (cd "$stand_dir" && apt download openssh-server && dpkg-deb -x openssh-server_*.deb "$package_dir")
fi
if [[ ! -e "$package_dir/usr/lib/x86_64-linux-gnu/libwrap.so.0" ]]; then
  (cd "$stand_dir" && apt download libwrap0 && dpkg-deb -x libwrap0_*.deb "$package_dir")
fi
export LD_LIBRARY_PATH="$package_dir/lib/x86_64-linux-gnu:$package_dir/usr/lib/x86_64-linux-gnu"

if [[ ! -e "$stand_dir/client-key" ]]; then
  ssh-keygen -q -t ed25519 -N '' -f "$stand_dir/client-key"
fi
chown "$stand_user" "$stand_dir/client-key" "$stand_dir/client-key.pub"
chmod 600 "$stand_dir/client-key"
cp "$stand_dir/client-key.pub" "$stand_dir/authorized_keys"
chmod 644 "$stand_dir/authorized_keys"

for name in edge_a edge_b target; do
  if [[ ! -e "$stand_dir/$name-host-key" ]]; then
    ssh-keygen -q -t ed25519 -N '' -f "$stand_dir/$name-host-key"
  fi
done

for spec in edge_a:37221 edge_b:37222 target:37223; do
  name="${spec%%:*}"
  port="${spec##*:}"
  cat > "$stand_dir/$name.sshd_config" <<EOF
Port $port
ListenAddress 127.0.0.1
HostKey $stand_dir/$name-host-key
PidFile $stand_dir/$name.pid
AuthorizedKeysFile $stand_dir/authorized_keys
AllowUsers $stand_user
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
StrictModes no
AllowTcpForwarding yes
PermitTTY yes
Subsystem sftp internal-sftp
LogLevel VERBOSE
EOF
  if ! is_running "$stand_dir/$name.pid"; then
    "$sshd_binary" -f "$stand_dir/$name.sshd_config" -E "$stand_dir/$name.log"
  fi
done

edge_a_pin="$(ssh-keygen -lf "$stand_dir/edge_a-host-key.pub" -E sha256 | awk '{print $2}')"
edge_b_pin="$(ssh-keygen -lf "$stand_dir/edge_b-host-key.pub" -E sha256 | awk '{print $2}')"
target_pin="$(ssh-keygen -lf "$stand_dir/target-host-key.pub" -E sha256 | awk '{print $2}')"

cat > "$stand_dir/config.toml" <<EOF
[settings]
connect_timeout_secs = 5
check_interval_secs = 10
max_parallel_routes = 3
chunk_size = 65536

[nodes.edge_a]
host = "127.0.0.1"
port = 37221
user = "$stand_user"
host_key = "$edge_a_pin"
key_file = "$stand_dir/client-key"

[nodes.edge_b]
host = "127.0.0.1"
port = 37222
user = "$stand_user"
host_key = "$edge_b_pin"
key_file = "$stand_dir/client-key"

[nodes.target]
host = "127.0.0.1"
port = 37223
user = "$stand_user"
host_key = "$target_pin"
key_file = "$stand_dir/client-key"

[[routes]]
name = "via-a"
target = "target"
hops = ["edge_a", "target"]
priority = 0

[[routes]]
name = "via-b"
target = "target"
hops = ["edge_b", "target"]
priority = 1

[[routes]]
name = "via-a-b"
target = "target"
hops = ["edge_a", "edge_b", "target"]
priority = 2
EOF
chown "$stand_user" "$stand_dir/config.toml"
chmod 600 "$stand_dir/config.toml"
mkdir -p "$stand_dir/data" "$stand_dir/state"
chown "$stand_user" "$stand_dir/data" "$stand_dir/state"
chmod 700 "$stand_dir/data" "$stand_dir/state"

echo "Stand ready: $stand_dir/config.toml"
