# Neo4j

DataHub uses Neo4j as graph db in the backend to serve graph queries.
[Official Neo4j image](https://hub.docker.com/_/neo4j) found in Docker Hub is used without 
any modification.

## Run Docker container
Below command will start all Neo4j container.
```
cd docker/neo4j && docker-compose pull && docker-compose up
```

## Container configuration
### External Port
If you need to configure default configurations for your container such as the exposed port, you will do that in
`docker-compose.yml` file. Refer to this [link](https://docs.docker.com/compose/compose-file/#ports) to understand
how to change your exposed port settings.
```
ports:
  - "7474:7474"
  - "7687:7687"
```

### Docker Network
All Docker containers for DataHub are supposed to be on the same Docker network which is `datahub_network`. 
If you change this, you will need to change it for all other Docker containers as well.
```
networks:
  default:
    name: datahub_network
```

## Neo4j Browser
To be able to debug and run Cypher queries against your Neo4j image, you can open up `Neo4j Browser` which is running at
[http://localhost:7474/browser/](http://localhost:7474/browser/). Default username is `neo4j` and password is `datahub`.