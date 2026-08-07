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

SERVER=":metadata-service:war,:datahub-upgrade,:metadata-jobs:mae-consumer-job,:metadata-jobs:mce-consumer-job"
ALL="${SERVER},:datahub-frontend,:datahub-actions"

echo "nothing that reaches an image"
check "smoke-test only" "" 'CHANGED_FILES=["smoke-test/tests/a_test.py"]'
check "docs and markdown" "" 'CHANGED_FILES=["docs/how/updating-datahub.md","README.md","docs-website/sidebars.js"]'
check "playwright tests" "" 'CHANGED_FILES=["e2e-test/ui/playwright/tests/a.spec.ts"]'
check "compose templates only" "" 'CHANGED_FILES=["docker/profiles/docker-compose.gms.yml"]'

echo "ui code does not rebuild the server"
check "datahub-web-react" ":datahub-frontend" 'CHANGED_FILES=["datahub-web-react/src/App.tsx"]'
check "datahub-frontend play app" ":datahub-frontend" 'CHANGED_FILES=["datahub-frontend/app/auth/Auth.java"]'
check "frontend dockerfile" ":datahub-frontend" 'CHANGED_FILES=["docker/datahub-frontend/Dockerfile"]'

echo "server business logic does not rebuild the ui"
check "metadata-io" "${SERVER}" 'CHANGED_FILES=["metadata-io/src/main/java/A.java"]'
check "metadata-jobs" "${SERVER}" 'CHANGED_FILES=["metadata-jobs/mae-consumer/src/main/java/A.java"]'
check "datahub-upgrade" "${SERVER}" 'CHANGED_FILES=["datahub-upgrade/src/main/java/A.java"]'
check "metadata-service internals" "${SERVER}" 'CHANGED_FILES=["metadata-service/factories/src/main/java/A.java"]'
check "graphql resolvers" "${SERVER}" 'CHANGED_FILES=["datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/A.java"]'

echo "the shared contract rebuilds both sides"
check "graphql schema resource" "${SERVER},:datahub-frontend" 'CHANGED_FILES=["datahub-graphql-core/src/main/resources/entity.graphql"]'
check "restli-client (frontend compiles it)" "${SERVER},:datahub-frontend" 'CHANGED_FILES=["metadata-service/restli-client/src/main/java/A.java"]'
check "entity-registry (frontend compiles it)" "${SERVER},:datahub-frontend" 'CHANGED_FILES=["entity-registry/src/main/java/A.java"]'
check "li-utils (frontend compiles it)" "${SERVER},:datahub-frontend" 'CHANGED_FILES=["li-utils/src/main/java/A.java"]'
check "metadata-models rebuilds everything" "${ALL}" 'CHANGED_FILES=["metadata-models/src/main/pegasus/com/linkedin/common/A.pdl"]'

echo "ingestion only rebuilds actions"
check "metadata-ingestion" ":datahub-actions" 'CHANGED_FILES=["metadata-ingestion/src/datahub/emitter/rest_emitter.py"]'
check "datahub-actions" ":datahub-actions" 'CHANGED_FILES=["datahub-actions/src/x.py"]'
check "shared docker snippets" ":datahub-actions" 'CHANGED_FILES=["docker/snippets/ingestion_base"]'

echo "unclassified paths force a full build"
check "root build.gradle" "${ALL}" 'CHANGED_FILES=["build.gradle"]'
check "smoke-test plus root build.gradle" "${ALL}" 'CHANGED_FILES=["smoke-test/tests/a_test.py","build.gradle"]'
check "gradle wrapper" "${ALL}" 'CHANGED_FILES=["gradle/wrapper/gradle-wrapper.properties"]'
check "buildSrc" "${ALL}" 'CHANGED_FILES=["buildSrc/src/main/java/A.java"]'
check "workflow definitions" "${ALL}" 'CHANGED_FILES=[".github/workflows/docker-unified.yml"]'
check "shared docker build machinery" "${ALL}" 'CHANGED_FILES=["docker/build.gradle"]'
check "a brand new top-level module" "${ALL}" 'CHANGED_FILES=["some-new-module/src/A.java"]'

echo "combinations union rather than override"
check "ui plus server business logic" "${SERVER},:datahub-frontend" 'CHANGED_FILES=["datahub-web-react/src/App.tsx","metadata-io/src/main/java/A.java"]'
check "ingestion plus ui" ":datahub-frontend,:datahub-actions" 'CHANGED_FILES=["datahub-web-react/src/App.tsx","metadata-ingestion/src/datahub/x.py"]'

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
