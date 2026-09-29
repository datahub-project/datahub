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

# Canonical CLI version = gradle/versioning/cliVersion.gradle (the same source the
# workflow uses for the host install and that GMS/docker images bundle). In CI the
# workflow exports the resolved value; for standalone/local runs, read it here.
CLI_VERSION="${CLI_VERSION:-$(sed -n 's/.*cliVersion = "\([^"]*\)".*/\1/p' "$REPO_ROOT/gradle/versioning/cliVersion.gradle" | head -1)}"

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
  # Gate on GITHUB_ACTIONS rather than CI: plain CI=true is exported by dev
  # containers, act-style local runners, and assorted tooling, and would nuke a
  # developer's local quickstart volumes (COMPOSE_PROJECT_NAME=datahub is the
  # same compose project).
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    datahub docker nuke || docker compose -p "$COMPOSE_PROJECT_NAME" down -v || true
  else
    # Never nuke outside CI: nuke irreversibly deletes the quickstart volumes.
    echo "Not running in GitHub Actions — leaving the stack up. Tear down with: datahub docker nuke"
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
# The heavy lifting (released-SDK install + source overlay + pytest + no-skip
# guard) lives in run-oauth-tester.sh, mounted at /smoke. CLI_VERSION pins the
# released base to the canonical version (see above).
docker exec -e "CLI_VERSION=$CLI_VERSION" "$TESTER" bash /smoke/run-oauth-tester.sh

echo "OAuth smoke test passed."
