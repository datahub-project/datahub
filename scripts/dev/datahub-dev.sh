#!/usr/bin/env bash
# Thin wrapper around datahub_dev.py so agents and humans can run:
#   scripts/datahub-dev.sh status
#   scripts/datahub-dev.sh rebuild --wait
# etc.
exec python3 "$(dirname "$0")/datahub_dev.py" "$@"
