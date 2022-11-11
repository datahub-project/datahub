#!/bin/bash

set -euo pipefail

# ASSUMPTION: This assumes that we're running from inside the venv.

SQLALCHEMY_VERSION=$(python -c 'import sqlalchemy; print(sqlalchemy.__version__)')

if [[ $SQLALCHEMY_VERSION == 1.3.* ]]; then
	ENSURE_NOT_INSTALLED=sqlalchemy2-stubs
	ENSURE_INSTALLED=sqlalchemy-stubs
elif [[ $SQLALCHEMY_VERSION == 1.4.* ]]; then
	ENSURE_NOT_INSTALLED=sqlalchemy-stubs
	ENSURE_INSTALLED=sqlalchemy2-stubs
else
	echo "Unsupported SQLAlchemy version: $SQLALCHEMY_VERSION"
	exit 1
fi

FORCE_REINSTALL=""
if pip show $ENSURE_NOT_INSTALLED >/dev/null 2>&1 ; then
	pip uninstall --yes $ENSURE_NOT_INSTALLED
	FORCE_REINSTALL="--force-reinstall"
fi

if [ -n "$FORCE_REINSTALL" ] || ! pip show $ENSURE_INSTALLED >/dev/null 2>&1 ; then
	pip install $FORCE_REINSTALL $ENSURE_INSTALLED
fi
