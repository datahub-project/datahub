# MariaDB

DataHub GMS can use MariaDB as an alternate storage backend.

[Official MariaDB Docker image](https://hub.docker.com/_/mariadb) found in Docker Hub is used without 
any modification.

## Run Docker container
Below command will start the MariaDB container.
```
cd docker/mariadb && docker-compose pull && docker-compose up
```

An initialization script [init.sql](init.sql) is provided to container. This script initializes `metadata-aspect` table
which is basically the Key-Value store of the DataHub GMS.

To connect to MariaDB container, you can type below command:
```
docker exec -it mariadb mysql -u datahub -pdatahub datahub
```

## Container configuration
### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - '3306:3306'
```

### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change this for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```