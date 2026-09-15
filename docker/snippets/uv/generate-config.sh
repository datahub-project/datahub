#!/bin/bash
# Generate uv.toml for the 'custom' (from-scratch) profile, or layer extra
# indexes onto an existing profile. Used by the Python 3.10-based images
# (datahub-ingestion) where tomllib is not available. Appending [[index]]
# blocks to a base profile is valid TOML and needs no parsing, so bash suffices.
#
# Env vars (URLs are creds-free; credentials live in ~/.netrc):
#   UV_PROFILE        'custom' (from-scratch) or an existing profile name.
#   BASE_PROFILE      Alias of UV_PROFILE (passed by the Dockerfile).
#   DEFAULT_INDEX_URL Default index URL (custom only; ignored for existing).
#   EXTRA_INDEX_URLS  Space-separated extra index URLs (both cases).
#   PROFILES_DIR      Directory holding <name>.toml base profiles.
#
# Writes to stdout; caller redirects to $HOME/.config/uv/uv.toml.
set -euo pipefail

UV_PROFILE="${UV_PROFILE:-${BASE_PROFILE:-}}"
PROFILES_DIR="${PROFILES_DIR:-}"
DEFAULT_INDEX_URL="${DEFAULT_INDEX_URL:-}"
EXTRA_INDEX_URLS="${EXTRA_INDEX_URLS:-}"

base_file="${PROFILES_DIR:+$PROFILES_DIR/}${UV_PROFILE}.toml"
# "custom" is an EXPLICIT choice — a missing profile file is NOT inferred as
# custom. An unknown profile (not "custom" and no file) is rejected so typos
# surface here instead of silently becoming a from-scratch build. This mirrors
# gradle's resolveUvBuildArgs, which the Dockerfiles bypass when they invoke
# this script directly (e.g. the Depot/direct-docker path).
is_custom=0
if [ "${UV_PROFILE}" = "custom" ]; then
  is_custom=1
elif [ -z "${base_file}" ] || [ ! -f "${base_file}" ]; then
  if [ -z "${base_file}" ]; then
    reason="PROFILES_DIR is unset"
  else
    reason="no profile file at ${base_file}"
  fi
  echo "generate-config.sh: unknown UV_PROFILE '${UV_PROFILE}' (${reason}). Set UV_PROFILE to 'custom' or an existing profile (default/chainguard/chainguard-ci)." >&2
  exit 1
fi

if [ "$is_custom" = 1 ]; then
  : "${DEFAULT_INDEX_URL:?DEFAULT_INDEX_URL is required for the custom profile}"
  cat <<EOF
index-strategy = "unsafe-best-match"

[[index]]
name = "custom-default"
url = "${DEFAULT_INDEX_URL}"
default = true
EOF
else
  # index-strategy is a top-level scalar and must precede any [[index]] blocks,
  # so prepend it (with a blank line) when the base profile doesn't set one.
  if ! grep -q '^index-strategy' "${base_file}"; then
    printf 'index-strategy = "unsafe-best-match"\n\n'
  fi
  cat "${base_file}"
fi

i=1
for url in ${EXTRA_INDEX_URLS}; do
  cat <<EOF

[[index]]
name = "extra-${i}"
url = "${url}"
EOF
  i=$((i + 1))
done
