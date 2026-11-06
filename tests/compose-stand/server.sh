#!/bin/sh
set -eu

: "${STAND_NAME:?STAND_NAME is required}"
test -r "/keys/$STAND_NAME"
mkdir -p /run/sshd /home/mssh/data
chown mssh:mssh /home/mssh /home/mssh/data

cat > /etc/ssh/sshd_config <<EOF
Port 22
ListenAddress 0.0.0.0
HostKey /keys/$STAND_NAME
AuthorizedKeysFile /keys/authorized_keys
AllowUsers mssh
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin no
AllowTcpForwarding yes
PermitTTY yes
StrictModes no
Subsystem sftp internal-sftp
LogLevel VERBOSE
EOF

peer_ip() {
    attempts=0
    while [ "$attempts" -lt 60 ]; do
        ip=$(getent hosts "$1" | awk 'NR == 1 { print $1 }')
        if [ -n "$ip" ]; then printf '%s\n' "$ip"; return 0; fi
        attempts=$((attempts + 1))
        sleep 1
    done
    echo "cannot resolve test peer $1" >&2
    return 1
}

peer_dev() {
    ip=$(peer_ip "$1")
    ip route get "$ip" | awk '{ for (i = 1; i <= NF; i++) if ($i == "dev") { print $(i + 1); exit } }'
}

shape() {
    dev=$1 delay=$2 rate=$3 loss=$4
    if [ "$rate" = 0 ]; then
        if [ "$loss" = 0 ]; then
            tc qdisc replace dev "$dev" root netem delay "$delay"
        else
            tc qdisc replace dev "$dev" root netem delay "$delay" loss "$loss"
        fi
    elif [ "$loss" = 0 ]; then
        tc qdisc replace dev "$dev" root netem delay "$delay" rate "$rate"
    else
        tc qdisc replace dev "$dev" root netem delay "$delay" rate "$rate" loss "$loss"
    fi
}

printf '%s\n' "${NETEM_RULES:-}" | tr ';' '\n' | while IFS=: read -r peer delay rate loss; do
    [ -n "$peer" ] || continue
    dev=$(peer_dev "$peer")
    [ -n "$dev" ] || { echo "no interface for $peer" >&2; exit 1; }
    shape "$dev" "$delay" "$rate" "$loss"
    echo "$STAND_NAME -> $peer on $dev: delay=$delay rate=$rate loss=$loss"
done

# Relay 3 has a reproducible burst of loss every 30 seconds, in addition to
# its low background loss. The other routes provide clean comparison paths.
if [ "${PERIODIC_LOSS_PEER:-}" ]; then
    burst_dev=$(peer_dev "$PERIODIC_LOSS_PEER")
    (
        while :; do
            sleep 30
            shape "$burst_dev" "$PERIODIC_LOSS_DELAY" 1000kbit 3%
            sleep 4
            shape "$burst_dev" "$PERIODIC_LOSS_DELAY" 1000kbit 0.1%
        done
    ) &
fi

exec /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config
