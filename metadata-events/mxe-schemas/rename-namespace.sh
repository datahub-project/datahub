#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.


SCRIPT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]:-$0}" )" >/dev/null && pwd )"

# Rename all com.linkedin.* to com.linkedin.pegasus2avro.*, except for com.linkedin.avro2pegasus.*
find $SCRIPT_ROOT/../mxe-schemas/src/renamed -type f -print0 | \
xargs -0 perl -pi -e 's/com\.linkedin\.(?!avro2pegasus)/com\.linkedin\.pegasus2avro\./g'

# Rename com.linkedin.avro2pegasus.* to com.linkedin.*
find $SCRIPT_ROOT/../mxe-schemas/src/renamed -type f -print0 | \
xargs -0 perl -pi -e 's/com\.linkedin\.avro2pegasus\./com\.linkedin\./g'
