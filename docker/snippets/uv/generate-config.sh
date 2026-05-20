#!/bin/bash
# Generates uv.toml with a custom default index and optional extra indexes.
# Pass index URLs via env vars — never embed credentials in URLs; use ~/.netrc instead.
# Writes to stdout; caller redirects to $HOME/.config/uv/uv.toml.
set -euo pipefail

DEFAULT_INDEX_URL="${DEFAULT_INDEX_URL:?DEFAULT_INDEX_URL is required}"

cat <<EOF
[[index]]
name = "custom-default"
url = "${DEFAULT_INDEX_URL}"
default = true
EOF

i=1
for url in ${EXTRA_INDEX_URLS:-}; do
    cat <<EOF

[[index]]
name = "extra-${i}"
url = "${url}"
EOF
    i=$((i + 1))
done
