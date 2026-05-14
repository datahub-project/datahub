#!/usr/bin/env bash
# Thin wrapper around datahub_dev.py so agents and humans can run:
#   scripts/datahub-dev.sh status
#   scripts/datahub-dev.sh rebuild --wait
# etc.
exec uv run --python 3.11 --no-project "$(dirname "$0")/datahub_dev.py" "$@"
