#!/bin/bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

if [[ "${SKIP_INSTALL_DEV:-}" != "true" ]]; then
  ../gradlew :smoke-test:installDev
  set +x
  echo "Activating virtual environment"
  source venv/bin/activate
  set -x
fi

mkdir -p ~/.datahub/plugins/frontend/auth/
echo "test_user:test_pass" >> ~/.datahub/plugins/frontend/auth/user.props

# Generate temporary token signing keys
DATAHUB_TOKEN_SERVICE_SIGNING_KEY=$(openssl rand -base64 32)
DATAHUB_TOKEN_SERVICE_SALT=$(openssl rand -base64 32)

echo "DATAHUB_VERSION = $DATAHUB_VERSION"
DATAHUB_SEARCH_IMAGE="${DATAHUB_SEARCH_IMAGE:=opensearchproject/opensearch}"
DATAHUB_SEARCH_TAG="${DATAHUB_SEARCH_TAG:=2.19.3}"
XPACK_SECURITY_ENABLED="${XPACK_SECURITY_ENABLED:=plugins.security.disabled=true}"
ELASTICSEARCH_USE_SSL="${ELASTICSEARCH_USE_SSL:=false}"
USE_AWS_ELASTICSEARCH="${USE_AWS_ELASTICSEARCH:=true}"

quickstart_compose() {
  THEME_V2_DEFAULT=false \
  SHOW_HAS_SIBLINGS_FILTER=false \
  SHOW_SEARCH_BAR_AUTOCOMPLETE_REDESIGN=false \
  SHOW_INGESTION_PAGE_REDESIGN=true \
  SHOW_HOME_PAGE_REDESIGN=true \
  SEARCH_BAR_API_VARIANT=AUTOCOMPLETE_FOR_MULTIPLE \
  DATAHUB_TELEMETRY_ENABLED=false \
  DEV_TOOLING_ENABLED=true \
  DOCKER_COMPOSE_BASE="file://$( dirname "$DIR" )" \
  DATAHUB_SEARCH_IMAGE="$DATAHUB_SEARCH_IMAGE" DATAHUB_SEARCH_TAG="$DATAHUB_SEARCH_TAG" \
  XPACK_SECURITY_ENABLED="$XPACK_SECURITY_ENABLED" ELASTICSEARCH_USE_SSL="$ELASTICSEARCH_USE_SSL" \
  USE_AWS_ELASTICSEARCH="$USE_AWS_ELASTICSEARCH" \
  DATAHUB_VERSION=${DATAHUB_VERSION} \
  ELASTICSEARCH_INDEX_BUILDER_REFRESH_INTERVAL_SECONDS=1 \
  KAFKA_LISTENER_CONCURRENCY=3 \
  POLICY_CACHE_REFRESH_INTERVAL_SECONDS=10 \
  DATAHUB_ACTIONS_IMAGE=acryldata/datahub-actions \
  DATAHUB_TOKEN_SERVICE_SIGNING_KEY=${DATAHUB_TOKEN_SERVICE_SIGNING_KEY} \
  DATAHUB_TOKEN_SERVICE_SALT=${DATAHUB_TOKEN_SERVICE_SALT} \
  DATAHUB_LOCAL_ACTIONS_ENV=$(pwd)/test_resources/actions/actions.env  \
  docker compose --project-directory ../docker/profiles --profile "${PROFILE_NAME:-quickstart-consumers}" "$@"
}

# CI can serve individual services from a previously published tag instead of
# rebuilding them (see .github/scripts/resolve_image_builds.sh), so print the
# tags compose actually resolved. A green test run against a stale image looks
# identical to a correct one without this.
echo "Resolved service images:"
resolved_images=$(quickstart_compose config --images)
echo "$resolved_images"

# A digest-pinned reference has to end at the digest. Trailing text means a
# compose template appended a variant suffix *outside* its ${VAR:-...}
# substitution, which the daemon rejects as "invalid reference format" only at
# `up` time -- minutes later, with no mention of which template is at fault.
# Fail here, where the offending reference can be named.
while read -r image; do
  [[ -n "${image}" ]] || continue
  if [[ "${image}" == *"@sha256:"* ]] && [[ ! "${image}" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "ERROR: malformed digest reference: ${image}" >&2
    echo "       A digest must be the last element of the reference. Text after it" >&2
    echo "       usually means a compose template appends a suffix outside the" >&2
    echo "       \${VAR:-...} substitution that carries the pinned digest." >&2
    exit 1
  fi
done <<<"${resolved_images}"

# EXPECT_PR_TAGGED_IMAGES lists the docker repos CI built for this run. Each one
# has to come up on DATAHUB_VERSION; if it resolved to the reused tag instead,
# the tests would run against binaries that predate the change and still pass,
# because the reused images are known-good by construction. That is the one
# failure this whole mechanism has to make loud, so fail here rather than hand a
# green run back. Unset outside CI, where nothing is reused.
for repo in ${EXPECT_PR_TAGGED_IMAGES:-}; do
  resolved=$(echo "$resolved_images" | grep "/${repo}:" || true)
  if [[ -z "$resolved" ]]; then
    if [[ "${PROFILE_NAME:-quickstart-consumers}" == "quickstart-consumers" ]]; then
      # Default profile: every expected repository must resolve. An absent
      # service here means compose configuration regressed, and skipping would
      # quietly bypass the stale-image assertion.
      echo "ERROR: ${repo} was built for this run but does not resolve to any" >&2
      echo "       service in the default compose profile." >&2
      exit 1
    fi
    # A non-default profile (smoke: label / dispatch) legitimately runs fewer
    # services (e.g. quickstartPg has no MAE/MCE); assert only what it runs.
    echo "NOTE: ${repo} is not part of profile ${PROFILE_NAME}; skipping tag assertion"
    continue
  fi
  if [[ "$resolved" != *":${DATAHUB_VERSION}"* ]]; then
    echo "ERROR: ${repo} was built for this run but resolved to '${resolved}'," >&2
    echo "       not a ${DATAHUB_VERSION} tag. The tests would run against a stale image." >&2
    exit 1
  fi
done

quickstart_compose up -d --quiet-pull --wait --wait-timeout 900

