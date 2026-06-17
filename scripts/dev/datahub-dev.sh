#!/usr/bin/env bash
# Thin wrapper around datahub_dev.py so agents and humans can run:
#   scripts/datahub-dev.sh status
#   scripts/datahub-dev.sh rebuild --wait
# etc.

# Read per-user config from ~/.datahub/dev/config.json.
# Reads two keys (runner, compose_project) in a single Python call to avoid
# paying the interpreter startup cost twice.
if [ "$DATAHUB_REMOTE_EXEC" != "1" ]; then
  _dh_cfg=$(python3 - <<'EOF' 2>/dev/null
import json, pathlib, sys
cfg = pathlib.Path.home() / ".datahub/dev/config.json"
try:
    d = json.loads(cfg.read_text())
    r = d.get("runner", "")
    p = d.get("compose_project", "")
    if r:
        r = str(pathlib.Path(r).expanduser())
    print(r)
    print(p)
except Exception:
    print("")
    print("")
EOF
)
  if [ -z "$DATAHUB_RUNNER" ]; then
    DATAHUB_RUNNER=$(echo "$_dh_cfg" | sed -n '1p')
    [ -n "$DATAHUB_RUNNER" ] && export DATAHUB_RUNNER
  fi
  if [ -z "$COMPOSE_PROJECT_NAME" ]; then
    COMPOSE_PROJECT_NAME=$(echo "$_dh_cfg" | sed -n '2p')
    [ -n "$COMPOSE_PROJECT_NAME" ] && export COMPOSE_PROJECT_NAME
  fi
  unset _dh_cfg
fi

# When a runner is configured and we're not already on the remote side,
# proxy ALL commands through the runner — EXCEPT the ones that are inherently
# local (managing the local registry or printing local connection info).
# 'start' and 'setup --remote' are handled specially inside datahub_dev.py
# itself, so they also skip this proxy and reach Python directly.
if [ -n "$DATAHUB_RUNNER" ] && [ "$DATAHUB_REMOTE_EXEC" != "1" ]; then
  case "${1:-}" in
    instances|shell-env|start|setup|suspend)
      # Local-aware commands: let Python handle them (start/setup/suspend have
      # their own runner logic; instances/shell-env read the local registry).
      ;;
    *)
      # Everything else (stop, reset, nuke, status, wait, rebuild, flag, env,
      # sync-flags, test, docs, frontend) runs on the remote.
      exec "$DATAHUB_RUNNER" exec -- \
        env DATAHUB_REMOTE_EXEC=1 "$(realpath "$0")" "$@"
      ;;
  esac
fi

exec uv run --python 3.11 --no-project "$(dirname "$0")/datahub_dev.py" "$@"
