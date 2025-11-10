#!/bin/bash
set -e

# Dynamically resolve FE and BE hostnames to IPs
FE_IP=$(getent hosts fe | awk '{ print $1 }')
BE_IP=$(getent hosts be | awk '{ print $1 }')

if [ -z "$FE_IP" ] || [ -z "$BE_IP" ]; then
    echo "ERROR: Could not resolve hostnames to IPs"
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

