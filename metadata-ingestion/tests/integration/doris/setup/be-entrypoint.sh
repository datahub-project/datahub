#!/bin/bash
set -e

# Wait for network to be ready and resolve hostnames
echo "Waiting for network to be ready..."
FE_IP=""
BE_IP=""
for i in {1..30}; do
    FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || true)
    BE_IP=$(getent hosts be 2>/dev/null | awk '{ print $1 }' || true)
    
    if [ -n "$FE_IP" ] && [ -n "$BE_IP" ]; then
        echo "Resolved FE hostname to IP: $FE_IP"
        echo "Resolved BE hostname to IP: $BE_IP"
        break
    fi
    echo "Attempt $i: Waiting for hostname resolution..."
    sleep 1
done

# If we couldn't resolve, just use the hostnames (Docker DNS will handle it)
if [ -z "$FE_IP" ]; then
    echo "Could not resolve 'fe' hostname to IP, using hostname directly"
    FE_IP="fe"
fi
if [ -z "$BE_IP" ]; then
    echo "Could not resolve 'be' hostname to IP, using hostname directly"
    BE_IP="be"
fi

# Set environment variables with resolved IPs
export FE_SERVERS="fe1:${FE_IP}:9010"
export BE_ADDR="${BE_IP}:9050"

echo "Starting Doris BE with FE_SERVERS=${FE_SERVERS} BE_ADDR=${BE_ADDR}"

# Call the original Doris entrypoint
exec bash entry_point.sh
