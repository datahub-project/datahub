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

