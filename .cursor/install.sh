#!/usr/bin/env bash
# Cloud Agent install script for DataHub.
#
# Installs the system-level toolchains DataHub's dev workflow needs, then
# bootstraps repo dev dependencies. This is intentionally idempotent so it can
# run repeatedly (or bake an environment build snapshot) without side effects.
#
# What the DataHub dev flow expects (see AGENTS.md):
#   - Docker + buildx + compose v2: `scripts/dev/datahub-dev.sh start` runs
#     `./gradlew quickstartDebug`, which builds debug images with `docker buildx
#     bake` and boots the full stack (GMS, frontend, MySQL, OpenSearch, Kafka)
#     via docker compose.
#   - uv: the `datahub-dev.sh` wrapper runs `uv run --python 3.11 ...`.
#   - mise: pins java 25 / node 22 / python 3.11 / yarn from mise.toml.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- System packages -------------------------------------------------------
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
# docker-buildx is required: quickstartDebug builds images via `docker buildx
# bake`, and without the plugin `docker buildx ...` fails with an obscure
# "unknown shorthand flag: 'f'" error.
# libkrb5-dev (+ build-essential/python3-dev/libsasl2-dev/libldap2-dev) is
# required to compile the gssapi/requests-gssapi wheels pulled in by
# acryl-datahub[dev]; without krb5-config the ingestion venv build fails.
sudo apt-get install -y --no-install-recommends \
  docker.io docker-buildx docker-compose-v2 \
  libkrb5-dev build-essential python3-dev libsasl2-dev libldap2-dev \
  git curl
# fuse3 ships an interactive conffile prompt; configure non-interactively so a
# partial dpkg state doesn't break later apt runs.
sudo DEBIAN_FRONTEND=noninteractive dpkg --configure -a --force-confold >/dev/null 2>&1 || true

# Let the current user reach the docker socket without sudo (start.sh also
# chmods the live socket for the current boot).
sudo groupadd -f docker
sudo usermod -aG docker "$USER" || true

# --- uv --------------------------------------------------------------------
export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# --- mise (pinned toolchains from mise.toml) -------------------------------
if ! command -v mise >/dev/null 2>&1; then
  curl -fsSL https://mise.run | sh
fi
# Persist tool activation for interactive shells (agent + humans).
# shellcheck disable=SC2016  # intentional: write the literals to .bashrc unexpanded
grep -q '.local/bin' "$HOME/.bashrc" 2>/dev/null || \
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
# shellcheck disable=SC2016  # intentional: write the literal command to .bashrc unexpanded
grep -q 'mise activate' "$HOME/.bashrc" 2>/dev/null || \
  echo 'eval "$(mise activate bash)"' >> "$HOME/.bashrc"

mise trust "$REPO_ROOT/mise.toml"
mise install
eval "$(mise activate bash)"

# --- Repo dev dependencies -------------------------------------------------
# datahub CLI + ingestion venv (metadata-ingestion/venv/bin/datahub).
scripts/dev/datahub-dev.sh setup
# Frontend node modules (datahub-web-react) for the React dev server / builds.
scripts/dev/datahub-dev.sh setup frontend

echo "DataHub install complete."
echo "Run 'scripts/dev/datahub-dev.sh start' to build and launch the full stack."
