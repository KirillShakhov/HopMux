#!/bin/sh
set -eu

mkdir -p /data
config=/data/config.toml
cat > "$config" <<'EOF'
direct_hosts = ["entry1", "entry2", "entry3", "entry4", "entry5"]
paths = [
  ["entry1", "relay1"],
  ["relay1", "hub"],
  ["entry2", "relay2"],
  ["relay2", "hub"],
  ["entry3", "relay3"],
  ["relay3", "hub"],
  ["entry4", "relay4"],
  ["relay4", "relay4b"],
  ["relay4b", "hub"],
  ["entry5", "relay5"],
  ["relay5", "relay5b"],
  ["relay5b", "hub"],
  ["hub", "target"],
]

[settings]
connect_timeout_secs = 20
check_interval_secs = 10
max_parallel_routes = 5
chunk_size = 1048576
EOF

for node in entry1 entry2 entry3 entry4 entry5 relay1 relay2 relay3 relay4 relay4b relay5 relay5b hub target; do
    pin=$(ssh-keygen -lf "/keys/$node.pub" -E sha256 | awk '{ print $2 }')
    cat >> "$config" <<EOF

[nodes.$node]
host = "$node"
user = "mssh"
host_key = "$pin"
key_file = "/keys/client"
EOF
done
chmod 600 "$config"

cargo build --locked
ln -sf /workspace/target/debug/mssh /usr/local/bin/mssh

# The client has one front interface. HTB selects a rate by entry IP; netem
# adds the client-side 50 ms delay to every path, including unclassified traffic.
front_ip=$(getent hosts entry1 | awk 'NR == 1 { print $1 }')
front_dev=$(ip route get "$front_ip" | awk '{ for (i = 1; i <= NF; i++) if ($i == "dev") { print $(i + 1); exit } }')
tc qdisc replace dev "$front_dev" root handle 1: htb default 99
tc class add dev "$front_dev" parent 1: classid 1:99 htb rate 100mbit
tc qdisc add dev "$front_dev" parent 1:99 handle 99: netem delay 50ms

for spec in 1:500kbit 2:750kbit 3:1000kbit 4:1250kbit 5:1500kbit; do
    i=${spec%%:*}
    rate=${spec#*:}
    ip=$(getent hosts "entry$i" | awk 'NR == 1 { print $1 }')
    class=$((10 + i))
    tc class add dev "$front_dev" parent 1: classid "1:$class" htb rate "$rate"
    tc qdisc add dev "$front_dev" parent "1:$class" handle "$class:" netem delay 50ms
    tc filter add dev "$front_dev" protocol ip parent 1: prio 1 u32 match ip dst "$ip/32" flowid "1:$class"
    echo "client -> entry$i: $rate, 50 ms egress delay"
done

echo "test client ready; config: $config"
exec sleep infinity
