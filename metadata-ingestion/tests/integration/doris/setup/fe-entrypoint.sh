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

# Workaround for Java 17 cgroup v2 incompatibility in GitHub Actions CI
# Doris's init_fe.sh ignores JAVA_OPTS, so we inject directly into fe.conf
FE_CONF="/opt/apache-doris/fe/conf/fe.conf"
if [ -f "$FE_CONF" ]; then
    # Add our JVM flags to the config file if not already present
    if ! grep -q "XX:-UseContainerSupport" "$FE_CONF" 2>/dev/null; then
        echo "Injecting cgroup v2 workaround into fe.conf"
        echo "JAVA_OPTS = \"-XX:-UseContainerSupport -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap\"" >> "$FE_CONF"
    fi
else
    echo "WARNING: fe.conf not found at $FE_CONF"
fi

# Call the original Doris entrypoint
exec bash init_fe.sh

