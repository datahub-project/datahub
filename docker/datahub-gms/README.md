# DataHub Generalized Metadata Store (GMS) Docker Image
[![datahub-gms docker](https://github.com/datahub-project/datahub/workflows/datahub-gms%20docker/badge.svg)](https://github.com/datahub-project/datahub/actions?query=workflow%3A%22datahub-gms+docker%22)

Refer to [DataHub GMS Service](../../metadata-service) to have a quick understanding of the architecture and 
responsibility of this service for the DataHub.

## Other Database Platforms

While GMS defaults to using MySQL as its storage backend, it is possible to switch to any of the 
[database platforms](https://ebean.io/docs/database/) supported by Ebean.

For example, you can run the following command to start a GMS that connects to a PostgreSQL backend.

```
(cd docker/ && docker-compose -f docker-compose.yml -f docker-compose.postgre.yml -p datahub up)
```

or a MariaDB backend

```
(cd docker/ && docker-compose -f docker-compose.yml -f docker-compose.mariadb.yml -p datahub up)
```
