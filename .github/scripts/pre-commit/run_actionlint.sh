#!/usr/bin/env bash
# Run the pinned actionlint version, preferring a local binary (mise.toml pins the same version)
# and falling back to Docker, so linting workflow files does not require a running Docker daemon.

set -euo pipefail

ACTIONLINT_VERSION="1.7.11"
ACTIONLINT_IMAGE="docker.io/rhysd/actionlint:${ACTIONLINT_VERSION}"

local_version=""
if command -v actionlint &>/dev/null; then
    local_version="$(actionlint -version | head -n 1)"
fi

if [[ "$local_version" == "$ACTIONLINT_VERSION" ]]; then
    exec actionlint "$@"
fi

if command -v docker &>/dev/null && docker info &>/dev/null; then
    exec docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/src" -w /src "$ACTIONLINT_IMAGE" "$@"
fi

# A different local version is better than no lint at all; CI runs the pinned check anyway.
if [[ -n "$local_version" ]]; then
    echo "warning: actionlint ${local_version} found, ${ACTIONLINT_VERSION} expected (run 'mise install')" >&2
    exec actionlint "$@"
fi

echo "error: actionlint not found on PATH and Docker is unavailable" >&2
echo "  Run 'mise install' to get actionlint ${ACTIONLINT_VERSION}, or start Docker." >&2
exit 1
