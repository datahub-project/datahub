#!/usr/bin/env bash
# Pre/post gzip of the Vite entry's static import closure, plus correctness checks.
# Intended to run inside the Node image (see the docker invocation at the bottom).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export CI=false
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=8192 --openssl-legacy-provider}"

BIN="$ROOT/node_modules/.bin"
OUT="${BUNDLE_OUT:-$ROOT/build/bundle-compare}"
REPO_ROOT="$ROOT/.."
BASE_COMMIT="${BUNDLE_BASE_COMMIT:-$(git -C "$REPO_ROOT" merge-base HEAD origin/master)}"
mkdir -p "$OUT"

if [[ ! -x "$BIN/vite" ]]; then
    echo "node_modules is missing. Install frontend dependencies before running this script." >&2
    exit 1
fi

# Drop a snapshot left by an earlier run before the trap can restore it.
rm -rf /tmp/src-post
SNAPSHOT_READY=0
restore_src() {
    if [[ "$SNAPSHOT_READY" != 1 || ! -d /tmp/src-post ]]; then
        return
    fi
    rm -rf "$ROOT/src"
    cp -a /tmp/src-post "$ROOT/src"
}
trap restore_src EXIT

echo "== generate GraphQL types and icon stubs =="
node scripts/generate-lazy-icon-stubs.js
"$BIN/graphql-codegen" --config codegen.yml

echo "== correctness: profile split + entity sidebar tests =="
"$BIN/vitest" run \
    src/app/entityV2/__tests__/profileChunkSplit.test.ts \
    src/app/entityV2/shared/__tests__/lazyEntityProfile.test.tsx \
    src/app/entityV2/glossaryTerm/__tests__/GlossaryTermEntity.test.tsx \
    src/app/entityV2/glossaryNode/__tests__/GlossaryNodeEntity.test.tsx \
    src/app/entityV2/shared/containers/profile/__tests__/utils.test.tsx \
    src/app/entityV2/__tests__/EntityRegistry.lineageVizConfig.test.ts

echo "== snapshot post source, then measure pre ($BASE_COMMIT) =="
rm -rf /tmp/src-post /tmp/src-pre
cp -a "$ROOT/src" /tmp/src-post
SNAPSHOT_READY=1
mkdir -p /tmp/src-pre
git -C "$REPO_ROOT" archive "$BASE_COMMIT" datahub-web-react/src \
    | tar -x -C /tmp/src-pre --strip-components=1
rm -rf "$ROOT/src"
cp -a /tmp/src-pre/src "$ROOT/src"
node scripts/generate-lazy-icon-stubs.js
"$BIN/graphql-codegen" --config codegen.yml

measure() {
    local label="$1"
    shift
    echo "== vite build ($label) =="
    rm -rf "$ROOT/dist"
    "$BIN/vite" build --mode production
    node scripts/measure-eager-bundle.mjs "$@" | tee "$OUT/${label}.json"
}

measure pre
restore_src
measure post --check-split

export BUNDLE_OUT="$OUT"
node --input-type=module -e '
import { readFileSync } from "node:fs";
const out = process.env.BUNDLE_OUT;
const pre = JSON.parse(readFileSync(out + "/pre.json", "utf8"));
const post = JSON.parse(readFileSync(out + "/post.json", "utf8"));
const kb = (n) => (Math.abs(n) / 1024).toFixed(0) + " KB";
const show = (n) => (n >= 1024 * 1024 ? (n / 1024 / 1024).toFixed(2) + " MB" : kb(n));
console.log("\nEager static closure (gzip -6)");
console.log("measure".padEnd(22), "pre".padStart(12), "post".padStart(12), "delta".padStart(12));
for (const key of ["entryGzip", "staticClosureGzip"]) {
    const delta = post[key] - pre[key];
    const sign = delta >= 0 ? "+" : "-";
    console.log(key.padEnd(22), show(pre[key]).padStart(12), show(post[key]).padStart(12), (sign + kb(delta)).padStart(12));
}
if (post.forbiddenInClosure?.length) {
    console.error("post build still eagerly imports profile UI", post.forbiddenInClosure);
    process.exit(1);
}
if (post.staticClosureGzip >= pre.staticClosureGzip) {
    console.error("post static closure is not smaller than pre");
    process.exit(1);
}
console.log("\npost entry does not statically include profile UI");
'

# docker (from the repo root, after frontend dependencies are installed):
#   docker run --rm -v "$PWD":/workspace -w /workspace/datahub-web-react node:22-bookworm \
#     bash -lc 'apt-get update -qq && apt-get install -y -qq git && bash scripts/compare-eager-bundle.sh'
