# Data Hub Quickstart
To start all Docker containers at once, please run below command:
```
cd docker/quickstart && docker-compose pull && docker-compose up
```
After containers are initialized, we need to create the `dataset` and `users` search indices by running below command:
```
cd docker/elasticsearch && bash init.sh
```
At this point, all containers are ready and Data Hub can be considered up and running. Check specific containers guide
for details:
* [Elasticsearch & Kibana](../elasticsearch)
* [Data Hub Frontend](../frontend)
* [Data Hub GMS](../gms)
* [Kafka, Schema Registry & Zookeeper](../kafka)
* [Data Hub MAE Consumer](../mae-consumer)
* [Data Hub MCE Consumer](../mce-consumer)
* [MySQL](../mysql) 

From this point on, if you want to be able to sign in to Data Hub and see some sample data, please see 
[Metadata Ingestion Guide](../../metadata-ingestion) for `bootstrapping Data Hub`.

## Debugging Containers
If you want to debug containers, you can check container logs:
```
docker logs <<container_name>>
```
Also, you can connect to container shell for further debugging:
```
docker exec -it <<container_name>> bash
```
