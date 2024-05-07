#!/bin/bash
set -euo pipefail

datahub ingest -c generated/analytics_docs/pipeline.yml
