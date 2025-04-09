#!/bin/bash

mkdir -p /tmp/datahub/logs

if [ "$DATAHUB_EXECUTOR_MODE" = "coordinator" ] || [ -z "$DATAHUB_EXECUTOR_MODE" ]; then
    echo "Starting datahub executor coordinator"
    exec uvicorn datahub_executor.coordinator.server:app --limit-concurrency ${UVICORN_CONCURRENCY:-10} --host 0.0.0.0 --port 9004 ${EXTRA_UVICORN_ARGS:-}
else
    echo "Starting datahub executor worker"
    exec watchmedo auto-restart -d /datahub-executor/ -p '*.py' -R -- celery -- -A datahub_executor.worker.celery_sqs.app -- worker -Q ${DATAHUB_EXECUTOR_POOL_ID:-$DATAHUB_EXECUTOR_WORKER_ID} -P solo --loglevel=info
fi
