#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../.." && pwd)"

GOOD=""
BAD="HEAD"
PERSONA="persona-p90-groups"
OPERATION="getMe"
THRESHOLD_P95_MS=""
ITERATIONS=10
RESULTS_DIR="${HOME}/.datahub/authz-perf-results/bisect-$(date +%F)"
GMS_URL="http://localhost:8080"
GMS_ONLY=""

usage() {
  cat <<EOF
Usage: bisect.sh --good REF --bad REF [options]

  --good REF              Known-good git ref (required)
  --bad REF               Known-bad git ref (default: HEAD)
  --persona NAME          Persona to benchmark (default: persona-p90-groups)
  --operation NAME        GraphQL operation (default: getMe)
  --threshold-p95-ms N    Fail if p95 exceeds N ms
  --threshold-p95-ratio R Use compare.py ratio mode instead (with --baseline)
  --baseline FILE         Baseline JSONL for ratio compare at each step
  --iterations N          Timed iterations (default: 10)
  --results-dir DIR       Output directory
  --gms-url URL           GMS URL (default: http://localhost:8080)
  --gms-only              Pass --gms-only to datahub-dev rebuild
EOF
}

BASELINE=""
THRESHOLD_P95_RATIO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --good) GOOD="$2"; shift 2 ;;
    --bad) BAD="$2"; shift 2 ;;
    --persona) PERSONA="$2"; shift 2 ;;
    --operation) OPERATION="$2"; shift 2 ;;
    --threshold-p95-ms) THRESHOLD_P95_MS="$2"; shift 2 ;;
    --threshold-p95-ratio) THRESHOLD_P95_RATIO="$2"; shift 2 ;;
    --baseline) BASELINE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --results-dir) RESULTS_DIR="$2"; shift 2 ;;
    --gms-url) GMS_URL="$2"; shift 2 ;;
    --gms-only) GMS_ONLY="--gms-only"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${GOOD}" ]]; then
  echo "--good is required" >&2
  exit 1
fi

mkdir -p "${RESULTS_DIR}"
"${ROOT}/setup.sh"

RUNNER="${RESULTS_DIR}/bisect-run.sh"
cat > "${RUNNER}" <<SCRIPT
#!/usr/bin/env bash
set -euo pipefail
cd "${REPO_ROOT}"
scripts/dev/datahub-dev.sh rebuild --wait ${GMS_ONLY}
OUT="${RESULTS_DIR}/commit-\$(git rev-parse --short HEAD).jsonl"
"${ROOT}/venv/bin/python" "${ROOT}/run.py" \\
  --gms-url "${GMS_URL}" \\
  --personas "${PERSONA}" \\
  --warmup 3 \\
  --iterations ${ITERATIONS} \\
  --cache-phases warm \\
  --parallel-personas 1 \\
  --output-dir "${RESULTS_DIR}"
SCRIPT
chmod +x "${RUNNER}"

VERIFY="${RESULTS_DIR}/bisect-verify.sh"
if [[ -n "${THRESHOLD_P95_MS}" ]]; then
  cat > "${VERIFY}" <<SCRIPT
#!/usr/bin/env bash
set -euo pipefail
OUT="${RESULTS_DIR}/commit-\$(git rev-parse --short HEAD).jsonl"
P95=\$(python3 -c "
import json, sys
from pathlib import Path
rows=[json.loads(l) for l in Path(sys.argv[1]).read_text().splitlines() if l.strip()]
rows=[r for r in rows if r.get('persona')=='${PERSONA}' and r.get('operation')=='${OPERATION}']
if not rows: sys.exit(1)
print(rows[-1]['stats']['p95'])
" "\${OUT}")
python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)" "\${P95}" "${THRESHOLD_P95_MS}"
SCRIPT
elif [[ -n "${BASELINE}" && -n "${THRESHOLD_P95_RATIO}" ]]; then
  cat > "${VERIFY}" <<SCRIPT
#!/usr/bin/env bash
set -euo pipefail
OUT="${RESULTS_DIR}/commit-\$(git rev-parse --short HEAD).jsonl"
"${ROOT}/venv/bin/python" "${ROOT}/compare.py" "${BASELINE}" "\${OUT}" \\
  --persona "${PERSONA}" --operation "${OPERATION}" \\
  --threshold-p95-ratio "${THRESHOLD_P95_RATIO}"
SCRIPT
else
  echo "Provide --threshold-p95-ms or (--baseline and --threshold-p95-ratio)" >&2
  exit 1
fi
chmod +x "${VERIFY}"

cd "${REPO_ROOT}"
git bisect reset 2>/dev/null || true
git bisect start "${BAD}" "${GOOD}"
git bisect run bash -c "${RUNNER} && ${VERIFY}"
git bisect reset

echo "[authz-perf] bisect complete; results in ${RESULTS_DIR}"
