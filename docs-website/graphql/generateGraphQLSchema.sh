#!/bin/sh
rm combined.graphql
touch combined.graphql
echo "Generating combined GraphQL schema..."
echo "# Auto Generated During Docs Build" >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/entity.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/search.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/app.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/recommendation.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/actions.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/constraints.graphql >> combined.graphql
cat ../../datahub-graphql-core/src/main/resources/auth.graphql >> combined.graphql

