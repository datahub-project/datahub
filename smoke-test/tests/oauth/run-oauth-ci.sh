#!/bin/bash
# CI entrypoint for the OAuth smoke test: brings up the quickstart stack + the
# Keycloak/GMS OAuth overlay, waits for readiness, runs the SDK OAuth test from
# inside the compose network, and always tears down.
#
# Assumes the `datahub` CLI is already installed (the workflow installs it) and
# Docker is available. Uses only public images.
set -euxo pipefail

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
HARNESS_DIR="$REPO_ROOT/smoke-test/tests/oauth"

# Absolute anchor so the overlay's bind mounts resolve regardless of which compose
# file Docker treats as the base (relative paths resolve against the FIRST -f file).
export DATAHUB_OAUTH_HARNESS_DIR="$HARNESS_DIR"
# Version mapping resolves `--version default` to pinned release image tags.
export FORCE_LOCAL_QUICKSTART_MAPPING="$REPO_ROOT/docker/quickstart/quickstart_version_mapping.yaml"
export COMPOSE_PROJECT_NAME=datahub

CLI_VERSION="$(grep 'cliVersion' "$REPO_ROOT/gradle/versioning/cliVersion.gradle" | head -1 | sed 's/.*"\([^"]*\)".*/\1/')"

teardown() {
  set +e
  echo "::group::docker ps + gms/keycloak logs (teardown)"
  docker ps -a
  docker logs "$(docker ps -aqf name=datahub-gms)" 2>&1 | tail -100 || true
  docker logs "$(docker ps -aqf name=keycloak)" 2>&1 | tail -50 || true
  echo "::endgroup::"
  if [ "${CI:-}" = "true" ]; then
    datahub docker nuke || docker compose -p "$COMPOSE_PROJECT_NAME" down -v || true
  else
    # Never nuke outside CI: COMPOSE_PROJECT_NAME=datahub is the same compose
    # project as a local quickstart, and nuke irreversibly deletes its volumes.
    echo "Not running in CI — leaving the stack up. Tear down with: datahub docker nuke"
  fi
}
trap teardown EXIT

echo "Bringing up quickstart + OAuth overlay (Keycloak + GMS external-OAuth)..."
datahub docker quickstart \
  -f "$REPO_ROOT/docker/quickstart/docker-compose.quickstart-profile.yml" \
  -f "$HARNESS_DIR/docker-compose.oauth.yml" \
  --version default

# Wait for Keycloak's JWKS (realm imported) — GMS can't validate tokens until this
# is reachable, and it's the slowest thing to come up.
echo "Waiting for Keycloak realm/JWKS..."
for i in $(seq 1 60); do
  if curl -sf "http://localhost:8083/realms/datahub/protocol/openid-connect/certs" >/dev/null; then
    echo "Keycloak realm ready."; break
  fi
  [ "$i" = "60" ] && { echo "Keycloak did not become ready"; exit 1; }
  sleep 5
done

# Wait for GMS health.
echo "Waiting for GMS health..."
for i in $(seq 1 60); do
  if curl -sf "http://localhost:8080/health" >/dev/null; then echo "GMS healthy."; break; fi
  [ "$i" = "60" ] && { echo "GMS did not become healthy"; exit 1; }
  sleep 5
done

# Run the SDK OAuth test from inside the network (issuer/JWKS hostnames stay
# internal + consistent — see README.md).
TESTER="$(docker ps -qf name=tester)"
[ -n "$TESTER" ] || { echo "tester container not found"; exit 1; }

echo "Running OAuth SDK smoke test in the tester container..."
# The tester needs the SDK auth layer (DatahubClientConfig.auth, first shipped in
# 1.6.0.9) — NOT the repo's default cliVersion, which can lag behind it. On an
# older SDK pydantic silently drops the test's `auth` config and the positive
# test fails with an unauthenticated 401 while the negative test passes
# vacuously. CLI_VERSION (above) is only used for `datahub docker quickstart`.
#
# The tests also skip themselves if the tester env vars are missing, and pytest
# exits 0 on all-skipped — so assert both tests actually PASSED.
docker exec "$TESTER" bash -c "
  set -euo pipefail
  pip install --quiet 'acryl-datahub>=1.6.0.9' pytest requests
  pytest /smoke/test_oauth_cli_gms.py -v 2>&1 | tee /tmp/pytest-oauth.out
  grep -q '2 passed' /tmp/pytest-oauth.out
"

echo "OAuth smoke test passed."
