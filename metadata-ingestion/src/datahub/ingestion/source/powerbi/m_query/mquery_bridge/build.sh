#!/usr/bin/env bash
# Builds bundle.js.gz for the PyMiniRacer M-Query bridge.
# Requires Node.js 16+ and npm.
# Run manually after bumping @microsoft/powerquery-parser in package.json.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Installing dependencies"
npm ci

echo "==> Compiling TypeScript"
npx tsc

echo "==> Bundling and minifying with esbuild"
npx esbuild dist/index.js \
  --bundle \
  --minify \
  --platform=node \
  --target=node16 \
  --outfile=bundle.js

echo "==> Compressing bundle"
gzip -9 -f bundle.js   # produces bundle.js.gz; -f overwrites existing

echo "    Written: bundle.js.gz ($(du -sh bundle.js.gz | cut -f1))"
echo ""
echo "==> Done. Commit bundle.js.gz and package-lock.json."
echo "    Then run generate_fixtures.py to regenerate test AST fixtures if needed."
