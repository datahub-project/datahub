#!/usr/bin/env bash
# End-to-end check of `datahub-dev.sh start --local`, run by the "GMS-lite Local Stack" workflow.
#
# Starts a real stack, then drives it only through the worktree's CLI profile: GraphQL `me`, a write
# that must reach search (MCP -> in-memory queue -> MCL -> OpenSearch), a warm restart that must
# refresh the profile's token, and a nuke that must remove everything the stack created.
#
# Expects the GMS/upgrade jars and the embedded OpenSearch distribution to be built, and the CLI at
# metadata-ingestion/venv. Run it with a throwaway HOME: it creates and deletes registry entries,
# profiles and data under $HOME/.datahub.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
DEV="scripts/dev/datahub-dev.sh"
DATAHUB="$ROOT/metadata-ingestion/venv/bin/datahub"
URN="urn:li:dataset:(urn:li:dataPlatform:hive,local_stack_e2e.events,PROD)"
DESCRIPTION="local-stack-e2e-$$"

step() { echo "::group::$*" 2>/dev/null || true; echo "== $*"; }
end_step() { echo "::endgroup::" 2>/dev/null || true; }
fail() { echo "::error::$*"; exit 1; }
# json_field a.b.c < doc.json prints doc["a"]["b"]["c"].
json_field() {
    python3 -c 'import functools, json, sys; print(functools.reduce(lambda d, k: d[k], sys.argv[1].split("."), json.load(sys.stdin)))' "$1"
}
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

step "start --local (cold)"
# The UI checks boot their own stack after this script's nuke.
"$DEV" start --local --fresh --no-frontend
end_step

step "status"
STATUS="$("$DEV" status)"
echo "$STATUS"
[[ "$(json_field mode <<<"$STATUS")" == "local" ]] || fail "status mode is not local"
[[ "$(json_field ready <<<"$STATUS")" == "True" ]] || fail "stack is not ready"
[[ "$(json_field services.gms-0.healthy <<<"$STATUS")" == "True" ]] || fail "GMS is not healthy"
PROFILE="$(json_field profile <<<"$STATUS")"
DATA_DIR="$(json_field data_dir <<<"$STATUS")"
PROFILE_FILE="$HOME/.datahub/profiles/$PROFILE/.datahubenv"
[[ -f "$PROFILE_FILE" ]] || fail "profile $PROFILE was not written"
end_step

step "CLI through the profile"
eval "$("$DEV" shell-env)"
[[ "${DATAHUB_PROFILE:-}" == "$PROFILE" ]] || fail "shell-env did not export DATAHUB_PROFILE=$PROFILE"
[[ -z "${DATAHUB_GMS_TOKEN:-}" ]] || fail "shell-env left DATAHUB_GMS_TOKEN set"
"$DATAHUB" profile current
ME="$("$DATAHUB" graphql --query '{ me { corpUser { urn } } }' --format json)"
echo "$ME"
[[ "$ME" == *'"urn:li:corpuser:datahub"'* ]] || fail "me did not resolve to the datahub user"
end_step

step "write reaches search"
printf '{"description": "%s"}\n' "$DESCRIPTION" >"$WORK/properties.json"
"$DATAHUB" put aspect --urn "$URN" --aspect datasetProperties --aspect-data "$WORK/properties.json"
"$DATAHUB" get --urn "$URN" --aspect datasetProperties | grep -q "$DESCRIPTION" || fail "get did not return the written aspect"
for _ in $(seq 1 30); do
    if "$DATAHUB" search "$DESCRIPTION" --format json 2>/dev/null | grep -q "local_stack_e2e.events"; then
        FOUND=1
        break
    fi
    sleep 2
done
[[ "${FOUND:-}" == 1 ]] || fail "the written dataset never became searchable"
end_step

step "stop, then warm start refreshes the token"
TOKEN_BEFORE="$(cksum <"$PROFILE_FILE")"
"$DEV" stop
[[ -f "$PROFILE_FILE" ]] || fail "stop removed the profile"
sleep 1 # token issue time has one-second resolution
"$DEV" start --local
[[ "$(cksum <"$PROFILE_FILE")" != "$TOKEN_BEFORE" ]] || fail "warm start did not refresh the profile token"
"$DATAHUB" get --urn "$URN" --aspect datasetProperties | grep -q "$DESCRIPTION" || fail "data did not survive the warm restart"
end_step

step "nuke removes what the stack created"
"$DEV" nuke
[[ ! -e "$DATA_DIR" ]] || fail "nuke left $DATA_DIR"
[[ ! -e "$PROFILE_FILE" ]] || fail "nuke left profile $PROFILE"
# Registry entries are keyed by worktree id, which is also the data dir's name.
if [[ -f "$HOME/.datahub/dev/instances.json" ]] &&
    python3 -c 'import json, sys; sys.exit(sys.argv[2] not in json.load(open(sys.argv[1]))["instances"])' \
        "$HOME/.datahub/dev/instances.json" "$(basename "$DATA_DIR")"; then
    fail "nuke left the registry entry"
fi
end_step

echo "local stack end-to-end check passed"
