#!/bin/bash

set -euxo pipefail

NO_DEV=false
if [[ ${1-} == '--no-dev' ]]; then
    NO_DEV=true
fi

echo 'Installing dependencies based on lockfile.'
if [[ "$NO_DEV" == false ]]; then
    uv pip sync requirements.txt requirements-dev.txt
else
    uv pip sync requirements.txt
fi

# We need the duckdb httpfs and aws extensions for the analytics source.
# We install them here because we don't want to be downloading/installing
# anything at runtime.
"$VIRTUAL_ENV/bin/python" -c "import duckdb; duckdb.execute('INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;')"
