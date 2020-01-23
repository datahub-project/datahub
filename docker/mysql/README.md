# MySQL

DataHub GMS uses MySQL as the storage infrastructure.
[Official MySQL Docker image](https://hub.docker.com/_/mysql) found in Docker Hub is used without 
any modification.

## Run Docker container
Below command will start the MySQL container.
```
cd docker/mysql && docker-compose pull && docker-compose up
```

An initialization script [init.sql](init.sql) is provided to container. This script initializes `metadata-aspect` table
which is basically the Key-Value store of the DataHub GMS.

To connect to MySQL container, you can type below command:
```
docker exec -it mysql mysql -u datahub -pdatahub datahub
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