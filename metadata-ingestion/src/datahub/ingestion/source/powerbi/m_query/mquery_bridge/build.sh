#!/usr/bin/env bash
# Builds Node.js SEA binaries for all supported platforms.
# Requires Node.js 20+ on the build machine.
# Run manually after bumping @microsoft/powerquery-parser version.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Installing dependencies"
npm ci

echo "==> Compiling TypeScript"
npx tsc

echo "==> Bundling with esbuild"
npx esbuild dist/index.js \
  --bundle \
  --platform=node \
  --target=node20 \
  --outfile=dist/bundle.js

echo "==> Generating SEA blob"
node --experimental-sea-config sea-config.json

# Helper: build one binary
build_binary() {
    local platform="$1"   # e.g. linux-x64
    local node_bin="$2"   # path to platform-specific node binary
    local out="binaries/mquery-parser-${platform}"

    echo "==> Building ${out}"
    cp "$node_bin" "$out"
    chmod +w "$out"

    # macOS requires removing existing signature before injection
    if [[ "$platform" == darwin-* ]]; then
        codesign --remove-signature "$out" || true
        npx postject "$out" NODE_SEA_BLOB sea-prep.blob \
            --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2 \
            --macho-segment-name NODE_SEA
    else
        npx postject "$out" NODE_SEA_BLOB sea-prep.blob \
            --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2
    fi

    # Re-sign ad-hoc on macOS (required for Gatekeeper)
    if [[ "$platform" == darwin-* ]]; then
        codesign --sign - "$out"
    fi

    # Make executable on Unix
    chmod +x "$out" 2>/dev/null || true

    echo "==> Compressing ${out}"
    gzip -9 --keep "$out"
    echo "    Written: ${out}.gz ($(du -sh "${out}.gz" | cut -f1))"
}

# Build for the current platform only (cross-compilation requires platform node binaries)
UNAME_S=$(uname -s | tr '[:upper:]' '[:lower:]')
UNAME_M=$(uname -m)

# Normalize machine name to match Python platform.machine() output
if [[ "$UNAME_M" == "arm64" ]]; then
    UNAME_M="arm64"
elif [[ "$UNAME_M" == "x86_64" ]]; then
    UNAME_M="x86_64"
fi

PLATFORM="${UNAME_S}-${UNAME_M}"
NODE_BIN=$(which node)

# Map to binary filename convention
case "$PLATFORM" in
    linux-x86_64)   build_binary "linux-x64"      "$NODE_BIN" ;;
    linux-aarch64)  build_binary "linux-aarch64"   "$NODE_BIN" ;;
    linux-arm64)    build_binary "linux-aarch64"   "$NODE_BIN" ;;
    darwin-x86_64)  build_binary "darwin-x64"      "$NODE_BIN" ;;
    darwin-arm64)   build_binary "darwin-arm64"    "$NODE_BIN" ;;
    *)
        echo "Unsupported platform: $PLATFORM"
        echo "Cross-compile by running build.sh on each target platform."
        exit 1
        ;;
esac

echo ""
echo "==> Done. Commit binaries/*.gz and package-lock.json."
echo "    Then run generate_fixtures.py to regenerate test AST fixtures."
