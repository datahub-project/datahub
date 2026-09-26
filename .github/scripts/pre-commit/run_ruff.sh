#!/usr/bin/env bash
# Wrapper that runs ruff locally when available, falling back to Docker.
# Used by pre-commit hooks so Docker is not required on every developer machine.

set -euo pipefail

RUFF_VERSION="0.15.22"
RUFF_IMAGE="ghcr.io/astral-sh/ruff:${RUFF_VERSION}"

# First arg is the ruff subcommand (check/format), rest are flags + files.
subcmd="$1"
shift

if command -v ruff &>/dev/null; then
    local_version=$(ruff --version 2>/dev/null | awk '{print $2}')
    # Warn if the local version is older than the pinned version.
    oldest=$(printf '%s\n%s\n' "$RUFF_VERSION" "$local_version" | sort -V | head -n1)
    if [ "$oldest" != "$RUFF_VERSION" ]; then
        echo "warning: local ruff ${local_version} is older than pinned ${RUFF_VERSION} — results may differ from CI" >&2
    fi
    exec ruff "$subcmd" "$@"
fi

# Fall back to Docker image.
if command -v docker &>/dev/null; then
    exec docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/src" -w /src "$RUFF_IMAGE" "$subcmd" "$@"
fi

echo "error: neither ruff nor docker found on PATH" >&2
echo "  Install ruff:   pip install ruff==${RUFF_VERSION}   (or: uv tool install ruff)" >&2
echo "  Or install Docker to use the containerised fallback." >&2
exit 1
