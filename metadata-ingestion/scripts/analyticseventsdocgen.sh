#!/bin/bash
set -euo pipefail

OUTDIR=./generated/analytics_docs

# Note: this assumes that datahub has already been built with `./gradlew build`.
DATAHUB_ROOT=..
ANALYTICS_EVENTS_FILE="$DATAHUB_ROOT/datahub-web-react/src/app/analytics/event.ts"

rm -r $OUTDIR || true
python scripts/generate_analytics_events.py --events-file $ANALYTICS_EVENTS_FILE --mces-file generated/analytics_docs/metadata_model_mces.json $@
