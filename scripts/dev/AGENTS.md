# scripts/dev — datahub-dev operations guide

Detail for `scripts/dev/datahub-dev.sh` that root `AGENTS.md` only signposts. How to run and
test the tool itself: `README.md` in this directory.

## Docker-free stack

Run `scripts/dev/datahub-dev.sh start --local` with JDK 25 to launch GMS on H2,
in-memory queues and a shared OpenSearch node. Select `--opensearch 2|3`; new stacks
use `EMBEDDED_OPENSEARCH_VERSION` or `embeddedOpenSearchDefault` in root `gradle.properties`
(currently 3). Restarts retain the recorded major; changing it requires `--fresh`.
Unmarked legacy stacks retain 3. Each major uses `~/.datahub/local/opensearch-<major>/`;
never use the unversioned `~/.datahub/local/opensearch/` directory. Status shows the
major, and cleanup follows it. Use `--processes N` for isolated
GMS instances, `--fresh` to reset this stack's data, `--auth` / `--no-auth` to enable
or disable auth (omitting both keeps the recorded setting), and `--skip-build`
to require existing artifacts (built jars are reused automatically).
Vite serves the React UI with hot reload on `9002 + slot * 1000`, using lite mode
without a Play server. Log in with `datahub` / `datahub`. `--no-frontend` skips Vite;
restarts retain this choice. Status includes Vite, and stop/nuke stop its recorded process
only after checking its command line. Data lives in `~/.datahub/local/stacks/<worktree-id>/`.

`status`, `wait`, `stop`, `reset`, `rebuild`, `env set/list/restart`, `instances list/clean`, `nuke`
and `shell-env` recognize local stacks. Local `nuke` removes this stack's data,
indices, profiles and registry entry without restarting; `nuke --keep-data` only
stops it and clears stale H2 locks. The shared search node survives both commands.
Unmarked local registry entries are read-only and keep their slots reserved.

Start refreshes CLI profiles `local-<worktree-id>` and `local-<worktree-id>-gms-N`
without changing the machine default. Select the default instance for this shell
with `eval "$(scripts/dev/datahub-dev.sh shell-env)"`. Stop retains the profiles.
See [README.md](README.md) for details and offline tests.

## Remote Runners

`datahub-dev.sh` supports a **runner plugin** that proxies operations to a remote machine
(EC2, Kubernetes pod, or any SSH-accessible host) instead of running Docker locally.

**Configure a runner** in `~/.datahub/dev/config.json`:

```json
{
  "max_local_instances": 2,
  "max_remote_instances": 10,
  "runner": "/path/to/your-runner.sh"
}
```

Or export `DATAHUB_RUNNER=/path/to/runner.sh` in your shell for a one-off session.

**Remote lifecycle** (all commands work identically to local once a runner is set):

```bash
# One-time bootstrap — provisions the remote environment
scripts/dev/datahub-dev.sh setup --remote

# Start — syncs changed local files, runs quickstartDebug on the remote,
#          then sets up port tunnels so local ports reach the remote instance
scripts/dev/datahub-dev.sh start

# Stop containers only (remote compute keeps running)
scripts/dev/datahub-dev.sh stop

# Stop containers AND halt the remote compute (no billing while suspended).
# 'start' will automatically resume the instance when needed.
scripts/dev/datahub-dev.sh suspend

# All other commands (status, wait, rebuild, test, flag, env, nuke, …)
# proxy through the runner transparently — use them exactly as you would locally.
scripts/dev/datahub-dev.sh status
```

**Multi-instance management** — each git worktree gets its own isolated instance
(separate Docker project, volumes, and port assignment):

```bash
# List all registered instances (local and remote) with their ports and status
scripts/dev/datahub-dev.sh instances list

# Remove stale entries for worktrees that no longer exist
scripts/dev/datahub-dev.sh instances clean

# Print export statements for the current instance's CLI environment
eval $(scripts/dev/datahub-dev.sh shell-env)
# → sets DATAHUB_GMS_URL to the correct local port (tunnel or direct)
```

**Port assignment** — each instance gets a slot; ports = base + slot × 1000:

| Slot | GMS   | Frontend | Notes                     |
| ---- | ----- | -------- | ------------------------- |
| 0    | 8080  | 9002     | First local instance      |
| 1    | 9080  | 10002    | Second local instance     |
| 2    | 10080 | 11002    | First remote instance     |
| …    | …     | …        | Each worktree is isolated |

**Backwards compatibility / opting out of isolation** — if the new per-worktree
project names cause problems (lost data in old volumes, tooling that expects
`datahub-*` container names, CI environments that don't need isolation), set
`compose_project` in `~/.datahub/dev/config.json`:

```json
{ "compose_project": "datahub" }
```

This reverts to the old single-instance behaviour: one `datahub` Docker project,
same container names, existing volumes fully accessible. The env var
`COMPOSE_PROJECT_NAME=datahub` has the same effect without touching the config file.

**Runner interface** — a runner is any executable that speaks four verbs:

```bash
runner init                        # one-time environment bootstrap
runner sync                        # push changed local files to the remote
runner exec -- <cmd> [args...]     # execute a command in the remote workspace
runner tunnel <local:remote> ...   # set up port forwarding
runner resume                      # start compute if stopped (no-op if running)
runner suspend                     # stop containers + halt compute
```

A reference Kubernetes runner is at `scripts/dev/runners/k8s.sh`.

## Recovery Escalation

**When to use each:**

- `stop`: Just shut down DataHub — no restart, no data loss
- `reset`: GMS returns 503 and doesn't recover, frontend shows "Unable to connect", tests fail
  with connection errors
- `nuke --keep-data`: Containers in restart loops, port conflicts, `reset` didn't fix it
- `nuke`: ES index corruption, MySQL schema issues after model changes, PDL model changes needing
  clean slate, `nuke --keep-data` didn't fix it

## Structured Test Output

Set `AGENT_MODE=1` to get machine-readable JSON test reports at `smoke-test/build/test-report.json`:

```bash
AGENT_MODE=1 scripts/dev/datahub-dev.sh test tests/test_system_info.py
```

For pytest smoke tests, see [`smoke-test/AGENTS.md`](../../smoke-test/AGENTS.md)
(authoring) and [`smoke-test/README.md`](../../smoke-test/README.md) (how to run).
