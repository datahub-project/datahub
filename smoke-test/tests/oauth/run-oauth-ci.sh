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
# The SDK under test is installed FROM THE REPO (the triggering ref), so this
# canary gates client-side auth changes — env-based DATAHUB_AUTH_TYPE
# resolution, the sink env-merge, precedence semantics — not just the released
# surface. GMS still runs the pinned release images (its OAuth validation is
# already shipped).
#
# Assertions: pytest's exit code (via pipefail) is the pass/fail signal; the
# greps additionally assert that nothing SKIPPED — the tests skip themselves
# when env vars are missing or the SDK lacks the auth layer, and pytest exits 0
# on an all-skipped run, which must not count as green.
docker exec "$TESTER" bash -c "
  set -euo pipefail
  pip install --quiet /repo/metadata-ingestion pytest requests
  pytest /smoke/test_oauth_cli_gms.py -v 2>&1 | tee /tmp/pytest-oauth.out
  grep -qE '[0-9]+ passed' /tmp/pytest-oauth.out
  ! grep -qE '[0-9]+ skipped|no tests ran' /tmp/pytest-oauth.out
"

echo "OAuth smoke test passed."
