#!/bin/sh
if [ -f "combined.graphql" ] ; then
    rm "combined.graphql"
fi
touch combined.graphql
echo "Generating combined GraphQL schema..."
echo "# Auto Generated During Docs Build" >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/*.graphql >> combined.graphql
