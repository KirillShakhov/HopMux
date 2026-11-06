#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
compose=(docker compose -f compose.test.yaml)
serve_exec_pid=""

cleanup() {
  "${compose[@]}" start entry4 entry5 >/dev/null 2>&1 || true
  "${compose[@]}" exec -T client sh -c 'if test -f /tmp/mssh-forward-test.pid; then kill "$(cat /tmp/mssh-forward-test.pid)" 2>/dev/null || true; rm -f /tmp/mssh-forward-test.pid; fi' >/dev/null 2>&1 || true
  if [[ -n "$serve_exec_pid" ]]; then wait "$serve_exec_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT

"${compose[@]}" exec -T client cargo build --locked
"${compose[@]}" exec -T client sh -c 'cp /data/config.toml /tmp/mssh-forward-test.toml; cat >> /tmp/mssh-forward-test.toml' <<'EOF'
local_ssh_port = 2222

[forwards.test_service]
via = "target"
listen = "127.0.0.1:2223"
remote = "127.0.0.1:22"
EOF
"${compose[@]}" exec -T client sh -c 'echo $$ >/tmp/mssh-forward-test.pid; exec /workspace/target/debug/mssh --config /tmp/mssh-forward-test.toml --debug --route target-4 --route target-5 serve >/tmp/mssh-forward-test.log 2>&1' &
serve_exec_pid=$!

wait_for_route() {
  local route="$1"
  local minimum="${2:-1}"
  for ((attempt=0; attempt<90; attempt++)); do
    count="$("${compose[@]}" exec -T client sh -c "grep -c 'forward test_service: ready .* -> ${route}: local ->' /tmp/mssh-forward-test.log || true")"
    if ((count >= minimum)); then
      return 0
    fi
    sleep 2
  done
  "${compose[@]}" exec -T client cat /tmp/mssh-forward-test.log
  return 1
}

check_ports() {
  for port in 2222 2223; do
    banner="$("${compose[@]}" exec -T client sh -c "timeout 25 nc -w 20 127.0.0.1 $port </dev/null | head -1")"
    [[ "$banner" == SSH-2.0-* ]] || { echo "missing SSH banner on port $port: $banner" >&2; return 1; }
  done
}

wait_for_route target-4
check_ports
"${compose[@]}" stop entry4
wait_for_route target-5
check_ports
"${compose[@]}" exec -T client sh -c 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -i /keys/client -p 2222 mssh@127.0.0.1 "printf forwarded_ssh_ok"' | grep -q forwarded_ssh_ok
"${compose[@]}" exec -T client sh -c 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -i /keys/client -p 2223 mssh@127.0.0.1 "printf forwarded_service_ok"' | grep -q forwarded_service_ok
"${compose[@]}" stop entry5
for ((attempt=0; attempt<45; attempt++)); do
  if "${compose[@]}" exec -T client sh -c "grep -q 'forward test_service: all routes unavailable' /tmp/mssh-forward-test.log"; then break; fi
  sleep 2
done
"${compose[@]}" exec -T client sh -c "grep -q 'forward test_service: all routes unavailable' /tmp/mssh-forward-test.log"
"${compose[@]}" exec -T client nc -z 127.0.0.1 2222
"${compose[@]}" start entry4
wait_for_route target-4 2
check_ports
echo 'Forwarded SSH and service ports recovered after route failover and a full outage.'
