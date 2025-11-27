#!/bin/bash
set -e

echo "Doris FE entrypoint starting..."

# Wait briefly for network to be ready
sleep 2

# Try to resolve hostname to IP, fallback if needed
FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || hostname -i || echo "")

if [ -z "$FE_IP" ]; then
    echo "WARNING: Could not resolve FE IP, using Docker internal IP"
    FE_IP=$(hostname -i)
fi

echo "Using FE IP: $FE_IP"

# Set FE_SERVERS with resolved IP
export FE_SERVERS="fe1:${FE_IP}:9010"
export FE_ID=1

echo "Starting Doris FE with FE_SERVERS=${FE_SERVERS} FE_ID=${FE_ID}"

# Ensure JAVA_OPTS is exported for the Doris startup script
# This workaround fixes Java 17 cgroup v2 incompatibility
if [ -n "$JAVA_OPTS" ]; then
    echo "Applying JAVA_OPTS: $JAVA_OPTS"
    export JAVA_OPTS
fi

# Call the original Doris entrypoint
exec bash init_fe.sh

