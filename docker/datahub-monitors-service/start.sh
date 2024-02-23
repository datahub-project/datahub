#!/bin/bash

mkdir -p /tmp/datahub/logs

if [ "$DATAHUB_EXECUTOR_MODE" = "coordinator" ] || [ -z "$DATAHUB_EXECUTOR_MODE" ]; then
    echo "Starting datahub monitors service/scheduler"
    uvicorn datahub_monitors.service.monitors_app.server:app --host 0.0.0.0 --port 9004 ${EXTRA_UVICORN_ARGS:-}
else
    echo "Starting datahub monitors worker"
    watchmedo auto-restart -d /datahub-monitors-service/ -p '*.py' -R -- celery -- -A datahub_monitors.workers.tasks.app -- worker -Q ${DATAHUB_EXECUTOR_WORKER_ID} -P threads --loglevel=info
fi
