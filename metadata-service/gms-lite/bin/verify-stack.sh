#!/usr/bin/env bash
# Verify real bootstrap, reads/writes, restart persistence, isolation, and shutdown.
set -euo pipefail
cd "$(dirname "$0")/../../.."
if [[ "${1:-}" != "--skip-build" ]]; then
  ./gradlew :metadata-service:war:bootJar :datahub-upgrade:bootJar \
    :metadata-service:opensearch-embedded:installDist -x test
fi
verification_dir=$(mktemp -d "${TMPDIR:-/tmp}/gms-lite-verify.XXXXXX")
echo "Logs and incremental results: $verification_dir"
python3 metadata-service/gms-lite/tests/stress_test_local_stack.py --data-dir "$verification_dir"
