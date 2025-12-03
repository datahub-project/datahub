#!/bin/bash
set -e

echo "Doris BE entrypoint starting..."

# Wait for FE to be somewhat ready
sleep 5

# Try to resolve hostnames to IPs, fallback if needed
FE_IP=$(getent hosts fe 2>/dev/null | awk '{ print $1 }' || echo "")
BE_IP=$(getent hosts be 2>/dev/null | awk '{ print $1 }' || hostname -i || echo "")

if [ -z "$FE_IP" ]; then
    echo "WARNING: Could not resolve FE IP via DNS, trying ping"
    FE_IP=$(ping -c 1 fe 2>/dev/null | grep -oP '\(\K[0-9.]+' || echo "fe")
fi

if [ -z "$BE_IP" ]; then
    echo "WARNING: Could not resolve BE IP, using Docker internal IP"
    BE_IP=$(hostname -i)
fi

echo "Using FE IP: $FE_IP"
echo "Using BE IP: $BE_IP"

# Set environment variables with resolved IPs
export FE_SERVERS="fe1:${FE_IP}:9010"
export BE_ADDR="${BE_IP}:9050"

echo "Starting Doris BE with FE_SERVERS=${FE_SERVERS} BE_ADDR=${BE_ADDR}"

# Workaround for Java 17 cgroup v2 incompatibility in GitHub Actions CI
# Doris's entry_point.sh ignores JAVA_OPTS, so we inject directly into be.conf
BE_CONF="/opt/apache-doris/be/conf/be.conf"
if [ -f "$BE_CONF" ]; then
    # Add our JVM flags to the config file if not already present
    if ! grep -q "XX:-UseContainerSupport" "$BE_CONF" 2>/dev/null; then
        echo "Injecting cgroup v2 workaround into be.conf"
        echo "JAVA_OPTS = \"-XX:-UseContainerSupport -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap\"" >> "$BE_CONF"
    fi
else
    echo "WARNING: be.conf not found at $BE_CONF"
fi

# Call the original Doris entrypoint
exec bash entry_point.sh

