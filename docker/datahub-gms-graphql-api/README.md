# Datahub GMS GraphQL API Docker Image

## Run

To start `Datahub GMS GraphQL API` along with all the dependencies

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up datahub-gms-graphql-api
```

To start `Datahub GMS GraphQL API` only without any dependencies

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up --no-deps datahub-gms-graphql-api
```

For more details refer [Docker README](../README.md)
