#!/usr/bin/env bash
# Run the pinned Ruff version with uv, falling back to Docker when uv is unavailable.

set -euo pipefail

RUFF_VERSION="0.15.22"
RUFF_IMAGE="ghcr.io/astral-sh/ruff:${RUFF_VERSION}"

# First arg is the ruff subcommand (check/format), rest are flags + files.
subcmd="$1"
shift

if command -v uv &>/dev/null; then
    exec uv run --isolated --with "ruff==${RUFF_VERSION}" -- ruff "$subcmd" "$@"
fi

# Fall back to Docker image.
if command -v docker &>/dev/null; then
    exec docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/src" -w /src "$RUFF_IMAGE" "$subcmd" "$@"
fi

echo "error: neither uv nor docker found on PATH" >&2
echo "  Install uv to run the pinned Ruff version, or install Docker." >&2
exit 1
