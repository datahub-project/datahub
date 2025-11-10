#!/bin/bash
set -e

# Dynamically resolve FE hostname to IP
FE_IP=$(getent hosts fe | awk '{ print $1 }')

if [ -z "$FE_IP" ]; then
    echo "ERROR: Could not resolve 'fe' hostname to IP"
    exit 1
fi

echo "Resolved FE hostname to IP: $FE_IP"

# Set FE_SERVERS with resolved IP
export FE_SERVERS="fe1:${FE_IP}:9010"
export FE_ID=1

echo "Starting Doris FE with FE_SERVERS=${FE_SERVERS}"

# Call the original Doris entrypoint
exec bash init_fe.sh

