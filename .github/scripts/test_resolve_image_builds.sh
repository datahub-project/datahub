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
# Registry lookups are served by a stub curl on PATH, so the real
# resolve_manifest_digest code path runs without network.
# Run: .github/scripts/test_resolve_image_builds.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNDER_TEST="${SCRIPT_DIR}/resolve_image_builds.sh"

STUB_DIR="$(mktemp -d)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${STUB_DIR}" "${WORK_DIR}"' EXIT

# Stub curl. Auth requests return a token; manifest requests succeed unless the
# tag is listed in MISSING_TAGS, which lets a test drive the "tag did not
# resolve" backstop without inventing a repo that does not exist. A successful
# manifest request emits a Docker-Content-Digest header derived from the tag
# name, so resolve_manifest_digest has something deterministic to extract and
# every tag gets a distinguishable (fake) digest.
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
tag="${url##*/manifests/}"
fake_hash=$(printf '%s' "${tag}" | cksum | cut -d' ' -f1)
echo "HTTP/1.1 200 OK"
printf 'Docker-Content-Digest: sha256:%064d\r\n' "${fake_hash}"
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

  if ! PATH="${STUB_DIR}:${PATH}" \
    GITHUB_OUTPUT="${out}" GITHUB_STEP_SUMMARY="${summary}" \
    env EVENT_NAME=pull_request FULL_BUILD_LABEL=false IS_FORK=false \
    PR_PUBLISH=false SMOKE_BUILD_TASK= \
    "$@" "${UNDER_TEST}" >/dev/null 2>&1; then
    failed=$((failed + 1))
    printf '  FAIL  %s\n        resolver exited nonzero\n' "${name}"
    return
  fi

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

# check_version_env <name> <expected image_version_env line> <env assignments...>
# Same harness as check(), asserting one line of the image_version_env heredoc
# output instead of image_build_modules -- covers the digest-pinning path that
# check() cannot see.
check_version_env() {
  local name="$1" expected="$2"
  shift 2

  local out="${WORK_DIR}/out" summary="${WORK_DIR}/summary"
  : >"${out}"
  : >"${summary}"

  if ! PATH="${STUB_DIR}:${PATH}" \
    GITHUB_OUTPUT="${out}" GITHUB_STEP_SUMMARY="${summary}" \
    env EVENT_NAME=pull_request FULL_BUILD_LABEL=false IS_FORK=false \
    PR_PUBLISH=false SMOKE_BUILD_TASK= \
    "$@" "${UNDER_TEST}" >/dev/null 2>&1; then
    failed=$((failed + 1))
    printf '  FAIL  %s\n        resolver exited nonzero\n' "${name}"
    return
  fi

  if grep -qF "${expected}" "${out}"; then
    passed=$((passed + 1))
    printf '  ok    %s\n' "${name}"
    return
  fi
  failed=$((failed + 1))
  printf '  FAIL  %s\n        expected image_version_env to contain: %s\n        actual image_version_env block:\n%s\n' \
    "${name}" "${expected}" "$(sed -n '/^image_version_env<</,/^IMAGE_VERSION_ENV_EOF$/p' "${out}")"
}

ALL=":metadata-service:war,:datahub-upgrade,:metadata-jobs:mae-consumer-job,:metadata-jobs:mce-consumer-job,:datahub-frontend,:datahub-actions"

echo "nothing that reaches an image"
check "smoke-test only" "" 'CHANGED_FILES=["smoke-test/tests/a_test.py"]'
check "docs and markdown" "" 'CHANGED_FILES=["docs/how/updating-datahub.md","README.md","docs-website/sidebars.js"]'
check "playwright tests" "" 'CHANGED_FILES=["e2e-test/ui/playwright/tests/a.spec.ts"]'
check "compose templates only" "" 'CHANGED_FILES=["docker/profiles/docker-compose.gms.yml"]'
check "quickstart version map" "" 'CHANGED_FILES=["docker/quickstart/quickstart_version_mapping.yaml"]'
check "datahub-agent-context" "" 'CHANGED_FILES=["datahub-agent-context/tests/unit/a.py"]'

echo "ingestion only rebuilds actions"
check "metadata-ingestion" ":datahub-actions" 'CHANGED_FILES=["metadata-ingestion/src/datahub/emitter/rest_emitter.py"]'
check "datahub-actions" ":datahub-actions" 'CHANGED_FILES=["datahub-actions/src/x.py"]'
check "actions readme (baked into image)" ":datahub-actions" 'CHANGED_FILES=["datahub-actions/README.md"]'

echo "reused images are pinned to a digest, not the bare floating tag"
check_version_env "reused image pinned to quickstart@digest" \
  "DATAHUB_GMS_VERSION=quickstart@sha256:0000000000000000000000000000000000000000000000000000003571886052" \
  'CHANGED_FILES=["metadata-ingestion/src/datahub/x.py"]'
check_version_env "reused -slim image pinned to its own digest" \
  "DATAHUB_ACTIONS_VERSION=quickstart-slim@sha256:0000000000000000000000000000000000000000000000000000004119348442" \
  'CHANGED_FILES=["smoke-test/tests/a_test.py"]'
check "ingestion plus docs" ":datahub-actions" 'CHANGED_FILES=["metadata-ingestion/src/datahub/x.py","docs/a.md","smoke-test/tests/b_test.py"]'

echo "any JVM-image path builds the full set (measured: narrowing saved nothing)"
check "datahub-web-react" "${ALL}" 'CHANGED_FILES=["datahub-web-react/src/App.tsx"]'
check "datahub-frontend play app" "${ALL}" 'CHANGED_FILES=["datahub-frontend/app/auth/Auth.java"]'
check "metadata-io" "${ALL}" 'CHANGED_FILES=["metadata-io/src/main/java/A.java"]'
check "metadata-jobs" "${ALL}" 'CHANGED_FILES=["metadata-jobs/mae-consumer/src/main/java/A.java"]'
check "metadata-service internals" "${ALL}" 'CHANGED_FILES=["metadata-service/factories/src/main/java/A.java"]'
check "graphql schema or resolvers" "${ALL}" 'CHANGED_FILES=["datahub-graphql-core/src/main/resources/entity.graphql"]'
check "metadata-models" "${ALL}" 'CHANGED_FILES=["metadata-models/src/main/pegasus/com/linkedin/common/A.pdl"]'
check "shared library" "${ALL}" 'CHANGED_FILES=["li-utils/src/main/java/A.java"]'
check "ingestion-scheduler" "${ALL}" 'CHANGED_FILES=["ingestion-scheduler/gradle.lockfile"]'
check "service dockerfile" "${ALL}" 'CHANGED_FILES=["docker/datahub-gms/Dockerfile"]'
# Every service Dockerfile COPYs these, not just the ingestion ones.
check "jvm-shared snippet" "${ALL}" 'CHANGED_FILES=["docker/snippets/setup_java_runtime.sh"]'
check "jvm-shared snippet (wait_for_deps)" "${ALL}" 'CHANGED_FILES=["docker/snippets/wait_for_deps.sh"]'
check "ingestion-only snippet" ":datahub-actions" 'CHANGED_FILES=["docker/snippets/ingestion_base.template"]'
check "ingestion plus jvm" "${ALL}" 'CHANGED_FILES=["metadata-ingestion/src/datahub/x.py","metadata-io/src/main/java/A.java"]'

echo "unclassified paths force a full build"
check "root build.gradle" "${ALL}" 'CHANGED_FILES=["build.gradle"]'
check "smoke-test plus root build.gradle" "${ALL}" 'CHANGED_FILES=["smoke-test/tests/a_test.py","build.gradle"]'
check "gradle wrapper" "${ALL}" 'CHANGED_FILES=["gradle/wrapper/gradle-wrapper.properties"]'
check "buildSrc" "${ALL}" 'CHANGED_FILES=["buildSrc/src/main/java/A.java"]'
check "workflow definitions" "${ALL}" 'CHANGED_FILES=[".github/workflows/docker-unified.yml"]'
check "a brand new top-level module" "${ALL}" 'CHANGED_FILES=["some-new-module/src/A.java"]'

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
