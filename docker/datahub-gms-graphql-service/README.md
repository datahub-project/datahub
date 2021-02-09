# Datahub GMS GraphQL Service Docker Image

## Run

To start `Datahub GMS GraphQL Service` along with all the dependencies

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up datahub-gms-graphql-service
```

To start `Datahub GMS GraphQL Service` only without any dependencies

```
docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up --no-deps datahub-gms-graphql-service
```

For more details refer [Docker README](../README.md)
