#!/bin/bash

set -euo pipefail

TARGET_DIR="${1:?usage: rename-namespace.sh <renamed-avro-dir>}"

# Rename all com.linkedin.* to com.linkedin.pegasus2avro.*, except for com.linkedin.avro2pegasus.*
find "$TARGET_DIR" -type f -print0 | \
xargs -0 perl -pi -e 's/com\.linkedin\.(?!avro2pegasus)/com\.linkedin\.pegasus2avro\./g'

# Rename com.linkedin.avro2pegasus.* to com.linkedin.*
find "$TARGET_DIR" -type f -print0 | \
xargs -0 perl -pi -e 's/com\.linkedin\.avro2pegasus\./com\.linkedin\./g'
