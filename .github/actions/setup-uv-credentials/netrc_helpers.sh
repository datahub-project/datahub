#!/usr/bin/env bash
# Shared helpers for setup-uv-credentials. Sourced inline by action.yml so the
# functions execute in the same shell as the caller and can read $netrc.

strip_creds() {
  python3 -c "import sys, urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(netloc=p.hostname + (':' + str(p.port) if p.port else ''))))" "$1"
}

extract_creds() {
  python3 -c "import sys, urllib.parse as u; p=u.urlparse(sys.argv[1]); print((p.username or '') + '\t' + (p.password or '') + '\t' + (p.hostname or ''))" "$1"
}

add_netrc_from_url() {
  # If URL has embedded creds, append a netrc entry. Idempotent.
  local url="$1" user pass host
  [ -z "$url" ] && return 0
  IFS=$'\t' read -r user pass host < <(extract_creds "$url")
  if [ -n "$user" ] && [ -n "$pass" ] && [ -n "$host" ]; then
    if ! grep -qE "^machine $host " "$netrc" 2>/dev/null; then
      echo "machine $host login $user password $pass" >> "$netrc"
    fi
  fi
}
