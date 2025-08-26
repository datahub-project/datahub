#!/bin/bash

echo "Starting datahub integrations service..."

# Add -Xfrozen_modules=off flag when debugging is enabled to prevent frozen modules warning
if [ "${DEBUGPY_ENABLED:-}" = "true" ]; then
    echo "🔧 Debug mode enabled - adding -Xfrozen_modules=off flag"
    exec python -Xfrozen_modules=off -m uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 ${EXTRA_UVICORN_ARGS:-}
else
    exec uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 ${EXTRA_UVICORN_ARGS:-}
fi
