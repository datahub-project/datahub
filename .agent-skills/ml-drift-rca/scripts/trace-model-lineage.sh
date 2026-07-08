#!/usr/bin/env bash
# Walk upstream lineage from an mlModel to its source datasets and their owners.
#
# This is the traversal step of ML drift root-cause: given a degrading model,
# find the upstream tables (and owning teams) where the cause likely lives.
#
# Usage:
#   ./trace-model-lineage.sh <MODEL_URN> <GMS_URL> [GMS_TOKEN]
#
# Example:
#   ./trace-model-lineage.sh \
#     'urn:li:mlModel:(urn:li:dataPlatform:mlflow,my_model,PROD)' \
#     http://localhost:8080
#
# Output: one line per upstream entity, as "hop<N>  <type>  <urn>  owners=<...>".
#
# Requires: python3 (standard library only). Reads DATAHUB_GMS_TOKEN from the
# env if a token is not passed as the third argument.
#
# Exit codes:
#   0 = traversal ran
#   1 = request or GraphQL error
#   2 = missing arguments

set -euo pipefail

MODEL_URN="${1:-}"
GMS_URL="${2:-${DATAHUB_GMS_URL:-}}"
GMS_TOKEN="${3:-${DATAHUB_GMS_TOKEN:-}}"

if [[ -z "$MODEL_URN" || -z "$GMS_URL" ]]; then
  echo "usage: $0 <MODEL_URN> <GMS_URL> [GMS_TOKEN]" >&2
  exit 2
fi

python3 - "$MODEL_URN" "$GMS_URL" "$GMS_TOKEN" <<'PY'
import json, sys, urllib.request

urn, gms, token = sys.argv[1], sys.argv[2].rstrip("/"), sys.argv[3]

query = (
    "query($urn: String!) {"
    "  searchAcrossLineage(input: {urn: $urn, direction: UPSTREAM, query: \"*\", count: 100}) {"
    "    searchResults {"
    "      degree"
    "      entity {"
    "        urn type"
    "        ... on Dataset { ownership { owners { owner {"
    "          ... on CorpUser { urn } ... on CorpGroup { urn } } } } }"
    "      }"
    "    }"
    "  }"
    "}"
)

body = json.dumps({"query": query, "variables": {"urn": urn}}).encode()
req = urllib.request.Request(f"{gms}/api/graphql", data=body,
                            headers={"Content-Type": "application/json"})
if token:
    req.add_header("Authorization", f"Bearer {token}")

try:
    data = json.loads(urllib.request.urlopen(req, timeout=30).read())
except Exception as e:  # noqa: BLE001
    print(f"error: {e}", file=sys.stderr)
    sys.exit(1)

if data.get("errors"):
    print("graphql errors:", [e.get("message") for e in data["errors"]][:3], file=sys.stderr)
    sys.exit(1)

results = (((data.get("data") or {}).get("searchAcrossLineage") or {}).get("searchResults")) or []
if not results:
    print(f"no upstream lineage found for {urn}")
    sys.exit(0)

for r in sorted(results, key=lambda x: x.get("degree", 99)):
    e = r.get("entity") or {}
    owners = [
        (o.get("owner") or {}).get("urn")
        for o in (((e.get("ownership") or {}).get("owners")) or [])
        if (o.get("owner") or {}).get("urn")
    ]
    print(f"hop{r.get('degree', '?')}  {e.get('type', '')}  {e.get('urn', '')}  "
          f"owners={','.join(owners) or '-'}")
PY
