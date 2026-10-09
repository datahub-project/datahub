# Enable TCPS on 2484. Sourced by the image's runUserScripts.sh, so this
# file must not change the caller's shell options. The subshell isolates
# `set -e`; a failure exits runUserScripts and the test times out on 2484
# instead of connecting in plaintext.
#
# TCP stays on 127.0.0.1 only. PMON registers services over TCP, and a
# TCPS-only listener never learns the PDB service. Loopback is not published,
# so the test process cannot open a plaintext connection.

if ! (
    set -euo pipefail

    WALLET=/opt/oracle/oradata/tls-wallet
    WALLET_PWD=WalletPass1
    NET_ADMIN="${ORACLE_BASE_HOME:-$ORACLE_HOME}/network/admin"
    ORACLE_GROUP="$(id -gn oracle)"
    # These are symlinks into oradata/dbconfig. Edit the targets so a later
    # sed -i cannot replace the symlink with a new file the listener ignores.
    LISTENER=$(readlink -f "$NET_ADMIN/listener.ora")
    SQLNET=$(readlink -f "$NET_ADMIN/sqlnet.ora")

    rm -rf "$WALLET"
    mkdir -p "$WALLET" /opt/oracle/tls/wallet
    "$ORACLE_HOME/bin/orapki" wallet create \
        -wallet "$WALLET" -pwd "$WALLET_PWD" -auto_login
    "$ORACLE_HOME/bin/orapki" wallet add \
        -wallet "$WALLET" \
        -pwd "$WALLET_PWD" \
        -dn "CN=localhost" \
        -keysize 2048 \
        -self_signed \
        -validity 3650 \
        -sign_alg sha256
    # wallet add does not refresh cwallet.sso. Recreate the auto-login file
    # so the listener can open the wallet without the password.
    rm -f "$WALLET/cwallet.sso"
    "$ORACLE_HOME/bin/orapki" wallet create \
        -wallet "$WALLET" -pwd "$WALLET_PWD" -auto_login
    "$ORACLE_HOME/bin/orapki" wallet export \
        -wallet "$WALLET" \
        -pwd "$WALLET_PWD" \
        -dn "CN=localhost" \
        -cert /opt/oracle/tls/wallet/ewallet.pem

    chown -R "oracle:${ORACLE_GROUP}" "$WALLET"
    chmod 700 "$WALLET"
    chmod 644 /opt/oracle/tls/wallet/ewallet.pem

    if ! grep -q "PROTOCOL = TCPS" "$LISTENER"; then
        sed -i \
            's#(ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))#(ADDRESS = (PROTOCOL = TCP)(HOST = 127.0.0.1)(PORT = 1521))\n      (ADDRESS = (PROTOCOL = TCPS)(HOST = 0.0.0.0)(PORT = 2484))#' \
            "$LISTENER"
    fi
    if ! grep -q "PROTOCOL = TCPS" "$LISTENER"; then
        echo "listener.ora has no TCPS address after rewrite" >&2
        cat "$LISTENER" >&2
        exit 1
    fi
    if ! grep -q WALLET_LOCATION "$LISTENER"; then
        cat >> "$LISTENER" <<'EOF'

WALLET_LOCATION =
  (SOURCE =
    (METHOD = FILE)
    (METHOD_DATA =
      (DIRECTORY = /opt/oracle/oradata/tls-wallet)
    )
  )
SSL_CLIENT_AUTHENTICATION = FALSE
EOF
    fi
    if ! grep -q WALLET_LOCATION "$SQLNET"; then
        cat >> "$SQLNET" <<'EOF'

WALLET_LOCATION =
  (SOURCE =
    (METHOD = FILE)
    (METHOD_DATA =
      (DIRECTORY = /opt/oracle/oradata/tls-wallet)
    )
  )
SSL_CLIENT_AUTHENTICATION = FALSE
EOF
    fi

    su oracle -p -c "$ORACLE_HOME/bin/lsnrctl stop" || true
    su oracle -p -c "$ORACLE_HOME/bin/lsnrctl start"

    ready=0
    for _ in $(seq 1 60); do
        if su oracle -p -c "$ORACLE_HOME/bin/lsnrctl status" | grep -q XEPDB1; then
            ready=1
            break
        fi
        sleep 2
    done
    if [ "$ready" -ne 1 ]; then
        echo "listener did not register XEPDB1" >&2
        su oracle -p -c "$ORACLE_HOME/bin/lsnrctl status" >&2 || true
        exit 1
    fi
); then
    echo "Failed to enable Oracle TCPS" >&2
    exit 1
fi
