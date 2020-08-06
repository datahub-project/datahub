# Neo4j

DataHub uses Neo4j as graph db in the backend to serve graph queries.
[Official Neo4j image](https://hub.docker.com/_/neo4j) found in Docker Hub is used without 
any modification.

## Neo4j Browser
To be able to debug and run Cypher queries against your Neo4j image, you can open up `Neo4j Browser` which is running at
[http://localhost:7474/browser/](http://localhost:7474/browser/). Default username is `neo4j` and password is `datahub`.