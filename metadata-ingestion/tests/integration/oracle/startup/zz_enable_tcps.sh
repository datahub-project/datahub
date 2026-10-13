# Enable TCPS on 2484. Sourced by the image's runUserScripts.sh, so this
# file must not change the caller's shell options. The subshell isolates
# `set -e`. It is not the condition of an `if`: bash turns errexit off for
# that whole subshell, and a later successful lsnrctl would hide a failed
# orapki. A failure exits runUserScripts. The test waits for the ready file
# written below, instead of connecting in plaintext.
#
# The image user is oracle, not root. chown and `su oracle` fail for that
# user. The wallet is created here, so it is already owned by oracle, and
# lsnrctl runs as the current user.
#
# TCP stays on 0.0.0.0:1521 and is not published. PMON registers the PDB
# against that endpoint; binding it to 127.0.0.1 makes lsnrctl status never
# show xepdb1, so this script exits before the ready file. A TCPS-only
# listener never learns the service either.

(
    set -euo pipefail
    # The fixture reads this when the ready file never appears. docker logs
    # are not in the pytest output.
    exec > /opt/oracle/tls/setup.log 2>&1

    WALLET=/opt/oracle/oradata/tls-wallet
    WALLET_PWD=WalletPass1
    NET_ADMIN="${ORACLE_BASE_HOME:-$ORACLE_HOME}/network/admin"
    # These are symlinks into oradata/dbconfig. Edit the targets so a later
    # sed -i cannot replace the symlink with a new file the listener ignores.
    LISTENER=$(readlink -f "$NET_ADMIN/listener.ora")
    SQLNET=$(readlink -f "$NET_ADMIN/sqlnet.ora")

    rm -rf "$WALLET"
    mkdir -p "$WALLET" /opt/oracle/tls/wallet
    # This image's orapki prompts even when -pwd is set. Compose does not
    # attach a terminal, so a -pwd-only call reads EOF and exits before the
    # ready file is written. wallet create asks for the password twice.
    orapki() {
        local prompts=$1
        shift
        local input="" i
        for i in $(seq 1 "$prompts"); do
            input+="${WALLET_PWD}"$'\n'
        done
        printf '%s' "$input" | "$ORACLE_HOME/bin/orapki" "$@"
    }
    orapki 2 wallet create -wallet "$WALLET" -auto_login
    orapki 1 wallet add \
        -wallet "$WALLET" \
        -dn "CN=localhost" \
        -keysize 2048 \
        -self_signed \
        -validity 3650 \
        -sign_alg sha256
    # wallet add does not refresh cwallet.sso. Recreate the auto-login file
    # so the listener can open the wallet without the password. An existing
    # wallet asks for its password once.
    rm -f "$WALLET/cwallet.sso"
    orapki 1 wallet create -wallet "$WALLET" -auto_login
    orapki 1 wallet export \
        -wallet "$WALLET" \
        -dn "CN=localhost" \
        -cert /opt/oracle/tls/wallet/ewallet.pem

    chmod 700 "$WALLET"
    chmod 644 /opt/oracle/tls/wallet/ewallet.pem

    if ! grep -q "PROTOCOL = TCPS" "$LISTENER"; then
        # Same insertion as the image's configTcps.sh. Leave the TCP address
        # alone so PMON can still reach the listener.
        sed -i "/PROTOCOL = TCP/a\\
      (ADDRESS = (PROTOCOL = TCPS)(HOST = 0.0.0.0)(PORT = 2484))" \
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

    "$ORACLE_HOME/bin/lsnrctl" stop || true
    "$ORACLE_HOME/bin/lsnrctl" start
    # Registration does not happen on reload. Ask PMON immediately instead of
    # waiting out the registration interval.
    echo "alter system register;" | "$ORACLE_HOME/bin/sqlplus" -s / as sysdba

    ready=0
    for _ in $(seq 1 60); do
        # XE registers this service as xepdb1. A case-sensitive XEPDB1
        # never matches, so the ready file below would never be written.
        if "$ORACLE_HOME/bin/lsnrctl" status | grep -qi xepdb1; then
            ready=1
            break
        fi
        sleep 2
    done
    if [ "$ready" -ne 1 ]; then
        echo "listener did not register XEPDB1" >&2
        "$ORACLE_HOME/bin/lsnrctl" status >&2 || true
        exit 1
    fi
    # 2484 accepts connections at lsnrctl start, before PMON registers
    # XEPDB1. The test waits for this file instead of the port.
    touch /opt/oracle/tls/ready
)
status=$?
if [ "$status" -ne 0 ]; then
    echo "Failed to enable Oracle TCPS" >&2
    exit "$status"
fi
