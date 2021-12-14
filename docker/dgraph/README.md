# Dgraph

DataHub can use Dgraph as the graph database in the backend to serve graph queries.
An alternative to Dgraph for that purpose is [Neo4j](../neo4j).

The [Dgraph image](https://hub.docker.com/r/dgraph/dgraph) found in Docker Hub is used without any modification.

## Dgraph UI Ratel

You can use the cloud hosted Dgraph UI [Ratel](https://play.dgraph.io/?latest#) to connect to your Dgraph cluster,
run queries and visualize your graph data. Point the UI to [http://localhost:8082](http://localhost:8082).
