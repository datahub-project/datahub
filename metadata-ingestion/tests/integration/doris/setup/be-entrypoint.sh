#!/bin/bash
set -eo pipefail

# Wait for network to be ready and resolve hostnames
echo "Waiting for network to be ready..."
for i in {1..30}; do
    FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || echo "")
    BE_IP=$(getent hosts be 2>/dev/null | awk '{ print $1 }' || echo "")
    
    if [ -n "$FE_IP" ] && [ -n "$BE_IP" ]; then
        break
    fi
    echo "Attempt $i: Waiting for hostname resolution..."
    sleep 1
done

if [ -z "$FE_IP" ] || [ -z "$BE_IP" ]; then
    echo "ERROR: Could not resolve hostnames to IPs after 30 attempts"
    echo "FE_IP: $FE_IP"
    echo "BE_IP: $BE_IP"
    exit 1
fi

echo "Resolved FE hostname to IP: $FE_IP"
echo "Resolved BE hostname to IP: $BE_IP"

# Set environment variables with resolved IPs
export FE_SERVERS="fe1:${FE_IP}:9010"
export BE_ADDR="${BE_IP}:9050"

echo "Starting Doris BE with FE_SERVERS=${FE_SERVERS} and BE_ADDR=${BE_ADDR}"

# Call the original Doris entrypoint
exec bash entry_point.sh

