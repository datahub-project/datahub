#!/bin/bash

mkdir -p /tmp/datahub/logs

# Optional: Install extra packages at boot time
if [ -n "$DATAHUB_EXECUTOR_EXTRA_PACKAGES" ]; then
    echo "Installing extra packages: $DATAHUB_EXECUTOR_EXTRA_PACKAGES"
    if uv pip install $DATAHUB_EXECUTOR_EXTRA_PACKAGES; then
        echo "Successfully installed extra packages"
    else
        if [ "$DATAHUB_EXECUTOR_EXTRA_PACKAGES_STRICT" = "true" ]; then
            echo "ERROR: Failed to install extra packages (strict mode enabled)"
            exit 1
        else
            echo "WARNING: Failed to install extra packages, continuing anyway"
        fi
    fi
fi

if [ "$DATAHUB_EXECUTOR_MODE" = "coordinator" ] || [ -z "$DATAHUB_EXECUTOR_MODE" ]; then
    echo "Starting datahub executor coordinator"
    exec uvicorn datahub_executor.coordinator.server:app --limit-concurrency ${UVICORN_CONCURRENCY:-10} --host 0.0.0.0 --port 9004 ${EXTRA_UVICORN_ARGS:-}
else
    echo "Starting datahub executor worker"
    if [ -n "${DATAHUB_SMOKETEST_EXECUTOR_ID}" ]; then
      export DATAHUB_EXECUTOR_WORKER_ID="${DATAHUB_SMOKETEST_EXECUTOR_ID}"
    fi
    exec watchmedo auto-restart -d /datahub-executor/ -p '*.py' -R -- celery -- -A datahub_executor.worker.celery_sqs.app -- worker -Q ${DATAHUB_EXECUTOR_POOL_ID:-$DATAHUB_EXECUTOR_WORKER_ID} -P solo --loglevel=info
fi
