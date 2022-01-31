#!/bin/bash
set -euo pipefail

datahub ingest -c generated/docs/pipeline.yml
