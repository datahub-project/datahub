#!/bin/bash
# Scenario table for resolve_image_builds.sh.
#
# The decision that script makes is a pure function -- environment in, plan out
# -- so the whole combinatorial surface (path classification, the backstops, the
# per-image couplings) is testable here with no CI run and no registry.
#
# What this protects against: the prefix list in resolve_image_builds.sh drifting
# out of sync with the repo. A new top-level module that nobody classifies has to
# keep forcing a full build, and the only thing that notices is this test.
#
# Registry lookups are served by a stub curl on PATH, so the real tag_exists code
# path runs without network. Run: .github/scripts/test_resolve_image_builds.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/resolve_image_builds.sh"

STUB_DIR="$(mktemp -d)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${STUB_DIR}" "${WORK_DIR}"' EXIT

# Stub curl. Auth requests return a token; manifest requests succeed unless the
# tag is listed in MISSING_TAGS, which lets a test drive the "tag did not
# resolve" backstop without inventing a repo that does not exist.
cat >"${STUB_DIR}/curl" <<'STUB'
#!/bin/bash
url="${*: -1}"
if [[ "$url" == *auth.docker.io* ]]; then
  echo '{"token":"stub"}'
  exit 0
fi
for missing in ${MISSING_TAGS:-}; do
  [[ "$url" == *"/manifests/${missing}" ]] && exit 22
done
exit 0
STUB
chmod +x "${STUB_DIR}/curl"

passed=0
failed=0

# check <name> <expected modules> <env assignments...>
check() {
  local name="$1" expected="$2"
  shift 2

  local out="${WORK_DIR}/out" summary="${WORK_DIR}/summary"
  : >"${out}"
  : >"${summary}"

  PATH="${STUB_DIR}:${PATH}" \
    GITHUB_OUTPUT="${out}" GITHUB_STEP_SUMMARY="${summary}" \
    env EVENT_NAME=pull_request FULL_BUILD_LABEL=false IS_FORK=false \
    PR_PUBLISH=false SMOKE_BUILD_TASK= \
    FRONTEND_CHANGE=false BACKEND_CHANGE=false \
    INGESTION_CHANGE=false ACTIONS_CHANGE=false \
    "$@" "${UNDER_TEST}" >/dev/null 2>&1

  local actual
  actual=$(sed -n 's/^image_build_modules=//p' "${out}")

  if [[ "${actual}" == "${expected}" ]]; then
    passed=$((passed + 1))
    printf '  ok    %s\n' "${name}"
    return
  fi
  failed=$((failed + 1))
  printf '  FAIL  %s\n        expected: %s\n        actual:   %s\n' \
    "${name}" "${expected:-<none>}" "${actual:-<none>}"
}

ALL=":metadata-service:war,:datahub-upgrade,:metadata-jobs:mae-consumer-job,:metadata-jobs:mce-consumer-job,:datahub-frontend,:datahub-actions"

echo "reuse paths"
check "smoke-test only builds nothing" \
  "" 'CHANGED_FILES=["smoke-test/tests/a_test.py"]'
check "docs and markdown build nothing" \
  "" 'CHANGED_FILES=["docs/how/updating-datahub.md","README.md","docs-website/sidebars.js"]'
check "playwright tests build nothing" \
  "" 'CHANGED_FILES=["e2e-test/ui/playwright/tests/a.spec.ts"]'
check "frontend only builds the frontend" \
  ":datahub-frontend" FRONTEND_CHANGE=true 'CHANGED_FILES=["datahub-web-react/src/App.tsx"]'
check "ingestion only builds actions" \
  ":datahub-actions" INGESTION_CHANGE=true 'CHANGED_FILES=["metadata-ingestion/src/datahub/x.py"]'
check "actions only builds actions" \
  ":datahub-actions" ACTIONS_CHANGE=true 'CHANGED_FILES=["datahub-actions/src/x.py"]'

echo "couplings"
check "backend rebuilds every image" \
  "${ALL}" BACKEND_CHANGE=true 'CHANGED_FILES=["metadata-service/factories/a.java"]'
check "metadata-models rebuilds every image" \
  "${ALL}" BACKEND_CHANGE=true INGESTION_CHANGE=true 'CHANGED_FILES=["metadata-models/src/main/pegasus/a.pdl"]'
check "graphql schema rebuilds the frontend too" \
  "${ALL}" BACKEND_CHANGE=true 'CHANGED_FILES=["datahub-graphql-core/src/main/resources/entity.graphql"]'

echo "unclassified paths force a full build"
check "root build.gradle" \
  "${ALL}" 'CHANGED_FILES=["build.gradle"]'
check "smoke-test plus root build.gradle" \
  "${ALL}" 'CHANGED_FILES=["smoke-test/tests/a_test.py","build.gradle"]'
check "gradle wrapper" \
  "${ALL}" 'CHANGED_FILES=["gradle/wrapper/gradle-wrapper.properties"]'
check "buildSrc" \
  "${ALL}" 'CHANGED_FILES=["buildSrc/src/main/java/A.java"]'
check "workflow definitions" \
  "${ALL}" 'CHANGED_FILES=[".github/workflows/docker-unified.yml"]'
check "a brand new top-level module" \
  "${ALL}" 'CHANGED_FILES=["some-new-module/src/A.java"]'

echo "backstops"
check "build-images label" \
  "${ALL}" FULL_BUILD_LABEL=true 'CHANGED_FILES=["smoke-test/a.py"]'
check "push to master" \
  "${ALL}" EVENT_NAME=push 'CHANGED_FILES=["smoke-test/a.py"]'
check "workflow_dispatch" \
  "${ALL}" EVENT_NAME=workflow_dispatch 'CHANGED_FILES=["smoke-test/a.py"]'
check "fork pull request" \
  "${ALL}" IS_FORK=true 'CHANGED_FILES=["smoke-test/a.py"]'
check "publish label" \
  "${ALL}" PR_PUBLISH=true 'CHANGED_FILES=["smoke-test/a.py"]'
check "smoke: label" \
  "${ALL}" SMOKE_BUILD_TASK=:docker:buildImagesQuickstartPg 'CHANGED_FILES=["smoke-test/a.py"]'
check "empty changed-file list" \
  "${ALL}" 'CHANGED_FILES=[]'
check "unreadable changed-file list" \
  "${ALL}" 'CHANGED_FILES=not-json'
check "missing quickstart tag" \
  "${ALL}" MISSING_TAGS="quickstart quickstart-slim" 'CHANGED_FILES=["smoke-test/a.py"]'
check "missing tag for one image only" \
  ":datahub-actions" MISSING_TAGS="quickstart-slim" 'CHANGED_FILES=["smoke-test/a.py"]'

echo
echo "${passed} passed, ${failed} failed"
[[ "${failed}" -eq 0 ]]
