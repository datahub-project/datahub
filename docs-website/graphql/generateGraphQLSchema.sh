#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

if [ -f "combined.graphql" ] ; then
    rm "combined.graphql"
fi
touch combined.graphql
echo "Generating combined GraphQL schema..."
echo "# Auto Generated During Docs Build" >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/*.graphql >> combined.graphql
