#!/usr/bin/env bash
# Linear sync (writes optional machine-readable summary JSON for CI integrations).
# Keeps security-scan-linear-sync.yml thin for easier merges with upstream.
set -euo pipefail

python3 -m pip install --disable-pip-version-check -q "requests==2.32.5"

if [[ -z "${LINEAR_SECURITY_SCAN_API_KEY:-}" ]]; then
  echo "::error::Missing GitHub Actions secret LINEAR_SECURITY_SCAN_API_KEY (empty or unset). Add it under Settings → Secrets and variables → Actions. For organization secrets, open the secret and add this repository to \"Repository access\"."
  exit 1
fi

shopt -s nullglob
files=()
if [[ "${SCAN_WITH_TRIVY:-}" == "true" ]]; then
  for f in scan-reports/trivy/*.json; do
    [[ -f "${f}" ]] && files+=("${f}")
  done
fi
if [[ "${SCAN_WITH_GRYPE:-}" == "true" ]]; then
  for f in scan-reports/grype/*.json; do
    [[ -f "${f}" ]] && files+=("${f}")
  done
fi
raw_files=()
if [[ "${SCAN_WITH_GRYPE:-}" == "true" ]]; then
  for f in scan-reports-raw/grype/*.json; do
    [[ -f "${f}" ]] && raw_files+=("${f}")
  done
fi

if [[ ${#files[@]} -eq 0 ]]; then
  echo "::notice::No merged scan report JSONs (e.g. all matrix scanner jobs failed or produced no files). Skipping Linear sync."
  exit 0
fi

summary_json="${RUNNER_TEMP}/security-scan-linear-summary.json"
cmd=(python3 .github/scripts/security_scan_linear_sync.py --scanner trivy_grype)
cmd+=(--output-summary-json "${summary_json}")
cmd+=("${files[@]}")
if [[ ${#raw_files[@]} -gt 0 ]]; then
  for f in "${raw_files[@]}"; do
    cmd+=(--raw-report-paths "$f")
  done
fi
"${cmd[@]}"
