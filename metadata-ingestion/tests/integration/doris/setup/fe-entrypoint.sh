#!/bin/bash
set -e

# Wait for network to be ready and resolve hostname
echo "Waiting for network to be ready..."
FE_IP=""
for i in {1..30}; do
    FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || true)
    if [ -n "$FE_IP" ]; then
        echo "Resolved FE hostname to IP: $FE_IP"
        break
    fi
    echo "Attempt $i: Waiting for hostname resolution..."
    sleep 1
done

# If we couldn't resolve, just use the hostname (Docker DNS will handle it)
if [ -z "$FE_IP" ]; then
    echo "Could not resolve 'fe' hostname to IP, using hostname directly"
    FE_IP="fe"
fi

# Set FE_SERVERS with resolved IP
export FE_SERVERS="fe1:${FE_IP}:9010"
export FE_ID=1

echo "Starting Doris FE with FE_SERVERS=${FE_SERVERS} FE_ID=${FE_ID}"

# Call the original Doris entrypoint
exec bash init_fe.sh
