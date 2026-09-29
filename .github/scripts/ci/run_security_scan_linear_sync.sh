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
trivy_count=0 grype_count=0
if [[ "${SCAN_WITH_TRIVY:-}" == "true" ]]; then
  for f in scan-reports/trivy/*.json; do
    [[ -f "${f}" ]] && files+=("${f}") && trivy_count=$((trivy_count + 1))
  done
fi
if [[ "${SCAN_WITH_GRYPE:-}" == "true" ]]; then
  for f in scan-reports/grype/*.json; do
    [[ -f "${f}" ]] && files+=("${f}") && grype_count=$((grype_count + 1))
  done
fi
raw_files=()
if [[ "${SCAN_WITH_GRYPE:-}" == "true" ]]; then
  for f in scan-reports-raw/grype/*.json; do
    [[ -f "${f}" ]] && raw_files+=("${f}")
  done
fi
echo "Collected ${trivy_count} trivy + ${grype_count} grype merged report(s), ${#raw_files[@]} grype raw report(s)."

# A scanner whose matrix job succeeded must have produced report files; zero files here means
# they were lost between upload and download (e.g. artifact path/layout drift) — fail loudly
# instead of silently syncing partial results. A failed scanner job only warns: partial sync of
# the other scanner is still valuable, and the "Fail on scanner errors" step reds the run anyway.
check_scanner_files() {
  local scanner="$1" enabled="$2" result="$3" count="$4"
  [[ "${enabled}" == "true" && "${count}" -eq 0 ]] || return 0
  if [[ "${result}" == "success" ]]; then
    echo "::error::${scanner} scan job succeeded but no merged ${scanner} report JSONs were found; findings would be silently dropped (check artifact upload/download paths)."
    exit 1
  fi
  echo "::warning::No merged ${scanner} report JSONs (scan job result: ${result:-unknown}); syncing without ${scanner} findings."
}
check_scanner_files trivy "${SCAN_WITH_TRIVY:-}" "${SCAN_TRIVY_RESULT:-}" "${trivy_count}"
check_scanner_files grype "${SCAN_WITH_GRYPE:-}" "${SCAN_GRYPE_RESULT:-}" "${grype_count}"

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
