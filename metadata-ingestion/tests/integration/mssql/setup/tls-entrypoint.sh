#!/usr/bin/env bash
# Install the fixture's certificate, then hand off to the image entrypoint.
# SQL Server reads tlscert/tlskey at startup. forceencryption is what makes a
# plaintext login fail instead of being silently accepted.
#
# The image user is mssql, and /var/opt/mssql is group-writable by mssql, so
# this does not need root. Replacing launch_sqlservr.sh would skip its
# permissions check.
set -euo pipefail

cert_dir=/var/opt/mssql/certs
mkdir -p "$cert_dir"
# Leaf plus CA. SQL Server wants the chain in tlscert and only the leaf key.
cat /tls/server.crt /tls/ca.crt > "$cert_dir/server.crt"
cp /tls/server.key "$cert_dir/server.key"
cat > /var/opt/mssql/mssql.conf <<'EOF'
[network]
tlscert = /var/opt/mssql/certs/server.crt
tlskey = /var/opt/mssql/certs/server.key
tlsprotocols = 1.2
forceencryption = 1
EOF
chmod 644 "$cert_dir/server.crt" /var/opt/mssql/mssql.conf
chmod 600 "$cert_dir/server.key"

# The image CMD is /opt/mssql/bin/sqlservr. Compose does not keep it when
# entrypoint is overridden, and launch_sqlservr.sh treats an empty "$@" as
# a command that has already finished.
if [ "$#" -eq 0 ]; then
    set -- /opt/mssql/bin/sqlservr
fi
exec /opt/mssql/bin/launch_sqlservr.sh "$@"
