#!/bin/bash
set -eo pipefail

# Wait for network to be ready and resolve hostname
echo "Waiting for network to be ready..."
for i in {1..30}; do
    FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || echo "")
    if [ -n "$FE_IP" ]; then
        break
    fi
    echo "Attempt $i: Waiting for hostname resolution..."
    sleep 1
done

if [ -z "$FE_IP" ]; then
    echo "ERROR: Could not resolve 'fe' hostname to IP after 30 attempts"
    exit 1
fi

echo "Resolved FE hostname to IP: $FE_IP"

# Set FE_SERVERS with resolved IP
export FE_SERVERS="fe1:${FE_IP}:9010"
export FE_ID=1

echo "Starting Doris FE with FE_SERVERS=${FE_SERVERS}"

# Call the original Doris entrypoint
exec bash init_fe.sh

