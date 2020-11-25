#!/bin/sh

CONTAINERS="\
	broker \
	datahub-frontend \
	datahub-gms \
	datahub-mae-consumer \
	datahub-mce-consumer \
	elasticsearch \
	elasticsearch-setup \
	kafka-rest-proxy \
	kafka-setup \
	kafka-topics-ui \
	kibana \
	mysql \
	neo4j \
	schema-registry \
	schema-registry-ui \
	zookeeper \

"

for CONTAINER in "$CONTAINERS"; do
	docker rm -f -v $CONTAINER
done

docker volume rm -f $(docker volume ls -f name=datahub_  -q)

docker network rm datahub_network
