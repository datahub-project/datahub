#!/bin/bash

# Quickstarts DataHub by pulling all images from dockerhub and then running the containers locally. No images are
# built locally.
# Note: by default this pulls the latest (head) version or the tagged version if you checked out a release tag.
# You can change this to a specific version by setting the DATAHUB_VERSION environment variable.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Detect if this is a checkout of a tagged branch.
# If this is a tagged branch, use the tag as the default, otherwise default to head.
# If DATAHUB_VERSION is set, it takes precedence.
TAG_VERSION=$(cd $DIR && git name-rev --name-only --tags HEAD)
DEFAULT_VERSION=$(echo $TAG_VERSION | sed 's/undefined/head/')
export DATAHUB_VERSION=${DATAHUB_VERSION:-${DEFAULT_VERSION}}

echo "Quickstarting DataHub: version ${DATAHUB_VERSION}"
cd $DIR && docker-compose pull && docker-compose -p datahub up
