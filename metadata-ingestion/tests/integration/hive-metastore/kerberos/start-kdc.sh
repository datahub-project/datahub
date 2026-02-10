#!/bin/sh
set -e

echo "Starting Kerberos KDC with keytab initialization..."

# Run the original entrypoint in background (it starts supervisord)
/docker-entrypoint.sh &

# Wait for KDC to be ready
echo "Waiting for KDC to initialize..."
sleep 10

# Check if KDC is running
i=1
while [ $i -le 20 ]; do
    if kadmin.local -q "listprincs" > /dev/null 2>&1; then
        echo "KDC is ready!"
        break
    fi
    echo "Waiting for KDC... attempt $i"
    sleep 2
    i=$((i + 1))
done

# Initialize keytabs
echo "Creating service principals and keytabs..."
/init-keytabs.sh

echo "KDC initialization complete."

# Keep the container running (wait for supervisord)
wait
