#!/bin/sh
set -eu
umask 077

mkdir -p /keys
if [ ! -s /keys/client ]; then
    ssh-keygen -q -t ed25519 -N '' -f /keys/client
fi
cp /keys/client.pub /keys/authorized_keys
chmod 644 /keys/authorized_keys

for node in entry1 entry2 entry3 entry4 entry5 relay1 relay2 relay3 relay4 relay4b relay5 relay5b hub target; do
    if [ ! -s "/keys/$node" ]; then
        ssh-keygen -q -t ed25519 -N '' -f "/keys/$node"
    fi
done

echo 'test stand keys ready'
