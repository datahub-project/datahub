# Metadata GraphQL API Docker Image

## Run
To Start `Metadata GraphQL API` along with all the dependencies

`docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up metadata-graphql-api`

To start `Metadata GraphQL API` only without any dependencies

`docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up --no-deps metadata-graphql-api`

For more details refer [README](../README.md)
