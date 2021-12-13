# Neo4j

DataHub can use Neo4j as the graph database in the backend to serve graph queries.
An alternative to Neo4j for that purpose is [Dgraph](../dgraph).

The [official Neo4j image](https://hub.docker.com/_/neo4j) found in Docker Hub is used without any modification.

## Neo4j Browser
To be able to debug and run Cypher queries against your Neo4j image, you can open up `Neo4j Browser` which is running at
[http://localhost:7474/browser/](http://localhost:7474/browser/). Default username is `neo4j` and password is `datahub`.