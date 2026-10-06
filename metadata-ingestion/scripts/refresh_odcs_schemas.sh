#!/usr/bin/env bash
# Refresh vendored ODCS JSON Schemas from upstream release tags.
# Usage: refresh_odcs_schemas.sh                # refresh all known versions
#        refresh_odcs_schemas.sh v3.1.0         # refresh only v3.1.0
#
# Verifies SHA-256 against CHECKSUMS.sha256. If a downloaded file's hash
# does not match, the script aborts with a non-zero exit and does NOT
# overwrite the vendored copy. To intentionally update the pinned hash,
# update CHECKSUMS.sha256 in the same commit as the schema change.
set -euo pipefail

UPSTREAM_REPO="bitol-io/open-data-contract-standard"

# (tag, upstream_filename, vendored_filename) tuples — one per known version.
KNOWN_VERSIONS=(
  "v3.0.2 odcs-json-schema-v3.0.2.json odcs-v3.0.2.json"
  "v3.1.0 odcs-json-schema-v3.1.0.json odcs-v3.1.0.json"
)

# Resolve the schema directory relative to this script's location, so the
# script works regardless of the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_DIR="$(cd "${SCRIPT_DIR}/../src/datahub/ingestion/source/odcs/odcs_schema" && pwd)"
CHECKSUM_FILE="${SCHEMA_DIR}/CHECKSUMS.sha256"

if [[ ! -f "${CHECKSUM_FILE}" ]]; then
  echo "error: checksum file not found: ${CHECKSUM_FILE}" >&2
  exit 1
fi

# Pick a SHA-256 implementation. macOS ships shasum; Linux typically ships
# sha256sum. Either produces the standard "<hex>  <filename>" output.
if command -v shasum >/dev/null 2>&1; then
  SHA256_CMD=(shasum -a 256)
elif command -v sha256sum >/dev/null 2>&1; then
  SHA256_CMD=(sha256sum)
else
  echo "error: neither 'shasum' nor 'sha256sum' is available on PATH" >&2
  exit 1
fi

requested_tag="${1:-}"

# Lookup the expected hash for a vendored filename from CHECKSUMS.sha256.
# Returns empty string if the filename is not present.
expected_hash_for() {
  local fname="$1"
  awk -v f="${fname}" '$2 == f { print $1; exit }' "${CHECKSUM_FILE}"
}

# Download a path at a given ref into a destination file. Prefers `gh api`
# (uses gh auth, avoids rate limits) and falls back to raw.githubusercontent.
download_at_ref() {
  local ref="$1" path="$2" dest="$3"
  if command -v gh >/dev/null 2>&1; then
    if gh api "repos/${UPSTREAM_REPO}/contents/${path}?ref=${ref}" \
         -H "Accept: application/vnd.github.raw" > "${dest}"; then
      return 0
    fi
    echo "warn: 'gh api' failed; falling back to raw.githubusercontent.com" >&2
  fi
  curl -fsSL \
    "https://raw.githubusercontent.com/${UPSTREAM_REPO}/${ref}/${path}" \
    -o "${dest}"
}

refresh_one() {
  local tag="$1" upstream_name="$2" vendored_name="$3"
  local vendored_path="${SCHEMA_DIR}/${vendored_name}"
  local tmp_file
  tmp_file="$(mktemp -t "odcs-${tag}.XXXXXX.json")"
  trap 'rm -f "${tmp_file}"' RETURN

  echo "==> ${vendored_name}: downloading ${UPSTREAM_REPO}@${tag}/schema/${upstream_name}"
  download_at_ref "${tag}" "schema/${upstream_name}" "${tmp_file}"

  local actual_hash
  actual_hash="$("${SHA256_CMD[@]}" "${tmp_file}" | awk '{print $1}')"

  local expected_hash
  expected_hash="$(expected_hash_for "${vendored_name}")"
  if [[ -z "${expected_hash}" ]]; then
    cat >&2 <<EOF
error: no entry for '${vendored_name}' in ${CHECKSUM_FILE}
       Add a line of the form '<sha256>  ${vendored_name}' deliberately
       and re-run this script.
EOF
    return 1
  fi

  if [[ "${actual_hash}" != "${expected_hash}" ]]; then
    cat >&2 <<EOF
error: SHA-256 mismatch for ${vendored_name} at upstream tag ${tag}
       expected: ${expected_hash}
       actual:   ${actual_hash}

The vendored copy at ${vendored_path} was NOT modified.

If this change is expected (e.g. you are deliberately updating the
pinned upstream version), update ${CHECKSUM_FILE} with the new hash
in the same commit and re-run this script.

If this change is unexpected, investigate possible upstream tampering
or transport corruption before accepting the new bytes.
EOF
    return 1
  fi

  mv "${tmp_file}" "${vendored_path}"
  echo "    ok: ${vendored_name} matches pinned hash ${expected_hash}"
}

found=0
for entry in "${KNOWN_VERSIONS[@]}"; do
  # shellcheck disable=SC2206
  parts=(${entry})
  tag="${parts[0]}"
  upstream_name="${parts[1]}"
  vendored_name="${parts[2]}"

  if [[ -n "${requested_tag}" && "${requested_tag}" != "${tag}" ]]; then
    continue
  fi

  refresh_one "${tag}" "${upstream_name}" "${vendored_name}"
  found=1
done

if [[ -n "${requested_tag}" && "${found}" -eq 0 ]]; then
  echo "error: unknown tag '${requested_tag}'. Known tags:" >&2
  for entry in "${KNOWN_VERSIONS[@]}"; do
    # shellcheck disable=SC2206
    parts=(${entry})
    echo "  - ${parts[0]}" >&2
  done
  exit 1
fi

echo "done."
