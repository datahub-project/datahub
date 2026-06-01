#!/usr/bin/env bash
# DataHub K8s Runner — reference implementation of the datahub-dev runner interface.
#
# Interface contract (called by datahub-dev.sh / datahub_dev.py):
#
#   runner init                        — one-time bootstrap of the remote environment
#   runner sync                        — incremental code sync to the pod
#   runner exec -- <cmd> [args...]     — execute a command in the remote workspace
#   runner tunnel <local:remote> ...   — set up host→pod port forwarding
#
# Configuration — override via env vars or edit the defaults below:
#
#   DATAHUB_K8S_POD         name of the dev pod          (default: datahub-dev)
#   DATAHUB_K8S_NAMESPACE   k8s namespace                (default: default)
#   DATAHUB_K8S_WORKDIR     working dir inside the pod   (default: /workspace/datahub)
#   DATAHUB_K8S_CONTEXT     kubectl context to use       (default: current context)
#
# Typical one-time setup:
#   export DATAHUB_RUNNER=/path/to/this/k8s.sh
#   datahub-dev.sh setup --remote      # calls: runner init
#   datahub-dev.sh start               # calls: runner sync + exec + tunnel
#
set -euo pipefail

POD="${DATAHUB_K8S_POD:-datahub-dev}"
NAMESPACE="${DATAHUB_K8S_NAMESPACE:-default}"
WORKDIR="${DATAHUB_K8S_WORKDIR:-/workspace/datahub}"
CONTEXT_ARG=()
if [ -n "${DATAHUB_K8S_CONTEXT:-}" ]; then
  CONTEXT_ARG=(--context "$DATAHUB_K8S_CONTEXT")
fi

KUBECTL=(kubectl "${CONTEXT_ARG[@]}" -n "$NAMESPACE")

_pod_exec() {
  # Run a shell command inside the pod's working directory.
  "${KUBECTL[@]}" exec "$POD" -- bash -c "cd $WORKDIR && $*"
}

case "${1:-}" in

  # ---------------------------------------------------------------------------
  init)
  # One-time bootstrap. Assumes the pod is already running (created via
  # Helm, terraform, or a cluster-admin). Installs Java, Docker-in-Docker
  # tooling, and performs an initial Gradle warm-up.
  # ---------------------------------------------------------------------------
    echo "[k8s-runner] Bootstrapping remote environment in pod/$POD ..."

    # Verify the pod is reachable.
    "${KUBECTL[@]}" get pod "$POD" --no-headers -o name

    # Ensure the workspace directory exists.
    _pod_exec "mkdir -p $WORKDIR"

    # Sync the current source tree so subsequent setup tasks have the code.
    echo "[k8s-runner] Syncing source for bootstrap..."
    "${KUBECTL[@]}" cp . "$POD:$WORKDIR"

    # Run DataHub's own local setup (ingestion venv, smoke-test venv, etc.)
    _pod_exec "scripts/dev/datahub-dev.sh setup ingestion"

    # Pre-pull the base Docker images so the first 'start' is faster.
    # Adjust the profile to match what you typically use.
    _pod_exec "./gradlew :docker:pullImages" || true

    echo "[k8s-runner] Remote environment ready."
    ;;

  # ---------------------------------------------------------------------------
  resume)
  # Ensure the pod exists and is running.  For K8s this typically means
  # scaling the deployment back up if it was scaled to zero.
  # ---------------------------------------------------------------------------
    echo "[k8s-runner] Resuming pod/$POD ..."
    # If the pod was created from a Deployment, scale it up.
    DEPLOY=$(kubectl get deployment -n "$NAMESPACE" -l app="$POD" \
               -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$DEPLOY" ]; then
      kubectl scale deployment -n "$NAMESPACE" "$DEPLOY" --replicas=1
      kubectl rollout status deployment -n "$NAMESPACE" "$DEPLOY" --timeout=120s
    else
      echo "[k8s-runner] No deployment found — assuming pod is already running."
    fi
    ;;

  # ---------------------------------------------------------------------------
  suspend)
  # Stop DataHub containers and scale the deployment to zero replicas.
  # No compute charges while scaled to zero; PVC data persists.
  # ---------------------------------------------------------------------------
    echo "[k8s-runner] Suspending pod/$POD ..."
    _pod_exec "env DATAHUB_REMOTE_EXEC=1 $WORKDIR/scripts/dev/datahub-dev.sh stop" || true
    DEPLOY=$(kubectl get deployment -n "$NAMESPACE" -l app="$POD" \
               -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$DEPLOY" ]; then
      kubectl scale deployment -n "$NAMESPACE" "$DEPLOY" --replicas=0
      echo "[k8s-runner] Deployment scaled to zero.  Resume with: datahub-dev.sh start"
    else
      echo "[k8s-runner] No deployment found to scale — containers left as-is."
    fi
    ;;

  # ---------------------------------------------------------------------------
  sync)
  # Incremental code sync. Uses kubectl cp with tar streaming.
  # For large repos consider mounting a shared PVC or using rsync over SSH
  # (kubectl port-forward + rsync daemon in the pod).
  # ---------------------------------------------------------------------------
    echo "[k8s-runner] Syncing code to pod/$POD ..."

    # Build a tar of tracked + untracked (but not gitignored) files.
    # This respects .gitignore and avoids shipping build artefacts.
    git ls-files --others --exclude-standard --cached -z \
      | tar --null -czf - --files-from=- \
      | "${KUBECTL[@]}" exec -i "$POD" -- bash -c \
          "mkdir -p $WORKDIR && tar -xzf - -C $WORKDIR"

    echo "[k8s-runner] Sync complete."
    ;;

  # ---------------------------------------------------------------------------
  exec)
  # Execute a command inside the pod's working directory.
  # Caller passes: runner exec -- <cmd> [args...]
  # The "--" separator is consumed here; the rest is forwarded verbatim.
  # ---------------------------------------------------------------------------
    shift          # drop "exec"
    if [ "${1:-}" = "--" ]; then shift; fi   # drop optional "--"
    _pod_exec "$*"
    ;;

  # ---------------------------------------------------------------------------
  tunnel)
  # Forward pod ports to localhost.  Each argument is "local:remote".
  # Runs kubectl port-forward in the foreground; press Ctrl-C to stop.
  #
  # Example invocation by datahub_dev.py:
  #   runner tunnel 28080:8080 29002:9002 23306:3306 ...
  # ---------------------------------------------------------------------------
    shift          # drop "tunnel"
    if [ $# -eq 0 ]; then
      echo "[k8s-runner] No ports specified for tunnel — nothing to do." >&2
      exit 0
    fi

    echo "[k8s-runner] Forwarding ports: $*"
    echo "[k8s-runner] Press Ctrl-C to stop port forwarding."
    # kubectl port-forward accepts "localPort:podPort" pairs natively.
    "${KUBECTL[@]}" port-forward "pod/$POD" "$@"
    ;;

  *)
    echo "Usage: $0 {init|sync|exec -- <cmd>|tunnel <local:remote>...}" >&2
    echo "" >&2
    echo "Environment variables:" >&2
    echo "  DATAHUB_K8S_POD        pod name          (default: datahub-dev)" >&2
    echo "  DATAHUB_K8S_NAMESPACE  namespace         (default: default)" >&2
    echo "  DATAHUB_K8S_WORKDIR    working directory (default: /workspace/datahub)" >&2
    echo "  DATAHUB_K8S_CONTEXT    kubectl context   (default: current)" >&2
    exit 1
    ;;

esac
