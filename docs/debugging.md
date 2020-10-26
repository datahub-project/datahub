# Debugging Guide

## How can I confirm if all Docker containers are running as expected after a quickstart?
You can list all Docker containers in your local by running `docker container ls`. You should expect to see a log similar to the below:

```
CONTAINER ID        IMAGE                                                 COMMAND                  CREATED             STATUS              PORTS                                                      NAMES
979830a342ce        linkedin/datahub-mce-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mce-consumer
3abfc72e205d        linkedin/datahub-frontend:latest                    "datahub-frontend/bi…"   10 hours ago        Up 10 hours         0.0.0.0:9001->9001/tcp                                     datahub-frontend
50b2308a8efd        linkedin/datahub-mae-consumer:latest                "bash -c 'while ping…"   10 hours ago        Up 10 hours                                                                    datahub-mae-consumer
4d6b03d77113        linkedin/datahub-gms:latest                         "bash -c 'dockerize …"   10 hours ago        Up 10 hours         0.0.0.0:8080->8080/tcp                                     datahub-gms
c267c287a235        landoop/schema-registry-ui:latest                     "/run.sh"                10 hours ago        Up 10 hours         0.0.0.0:8000->8000/tcp                                     schema-registry-ui
4b38899cc29a        confluentinc/cp-schema-registry:5.2.1                 "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:8081->8081/tcp                                     schema-registry
37c29781a263        confluentinc/cp-kafka:5.2.1                           "/etc/confluent/dock…"   10 hours ago        Up 10 hours         0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp           broker
15440d99a510        docker.elastic.co/kibana/kibana:5.6.8                 "/bin/bash /usr/loca…"   10 hours ago        Up 10 hours         0.0.0.0:5601->5601/tcp                                     kibana
943e60f9b4d0        neo4j:4.0.6                                           "/sbin/tini -g -- /d…"   10 hours ago        Up 10 hours         0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   neo4j
6d79b6f02735        confluentinc/cp-zookeeper:5.2.1                       "/etc/confluent/dock…"   10 hours ago        Up 10 hours         2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp                 zookeeper
491d9f2b2e9e        docker.elastic.co/elasticsearch/elasticsearch:5.6.8   "/bin/bash bin/es-do…"   10 hours ago        Up 10 hours         0.0.0.0:9200->9200/tcp, 9300/tcp                           elasticsearch
ce14b9758eb3        mysql:5.7
```

Also you can check individual Docker container logs by running `docker logs <<container_name>>`. For `datahub-gms`, you should see a log similar to this at the end of the initialization:
```
2020-02-06 09:20:54.870:INFO:oejs.Server:main: Started @18807ms
```

For `datahub-frontend`, you should see a log similar to this at the end of the initialization:
```
09:20:22 [main] INFO  play.core.server.AkkaHttpServer - Listening for HTTP on /0.0.0.0:9001
```

## My elasticsearch or broker container exited with error or was stuck forever

If you're seeing errors like below, chances are you didn't give enough resource to docker. Please make sure to allocate at least 8GB of RAM + 2GB swap space.
```
datahub-gms             | 2020/04/03 14:34:26 Problem with request: Get http://elasticsearch:9200: dial tcp 172.19.0.5:9200: connect: connection refused. Sleeping 1s
broker                  | [2020-04-03 14:34:42,398] INFO Client session timed out, have not heard from server in 6874ms for sessionid 0x10000023fa60002, closing socket connection and attempting reconnect (org.apache.zookeeper.ClientCnxn)
schema-registry         | [2020-04-03 14:34:48,518] WARN Client session timed out, have not heard from server in 20459ms for sessionid 0x10000023fa60007 (org.apache.zookeeper.ClientCnxn)
```

## How can I check if [MXE](what/mxe.md) Kafka topics are created?

You can use a utility like [kafkacat](https://github.com/edenhill/kafkacat) to list all topics. 
You can run below command to see the Kafka topics created in your Kafka broker.

```bash
kafkacat -L -b localhost:9092
```

Confirm that `MetadataChangeEvent` & `MetadataAuditEvent` topics exist besides the default ones. Example response as below:

```bash
Metadata for all topics (from broker 1: localhost:9092/1):
 1 brokers:
  broker 1 at localhost:9092
 5 topics:
  topic "_schemas" with 1 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
  topic "__consumer_offsets" with 50 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
    partition 1, leader 1, replicas: 1, isrs: 1
    partition 2, leader 1, replicas: 1, isrs: 1
    partition 3, leader 1, replicas: 1, isrs: 1
    partition 4, leader 1, replicas: 1, isrs: 1
    partition 5, leader 1, replicas: 1, isrs: 1
    partition 6, leader 1, replicas: 1, isrs: 1
    partition 7, leader 1, replicas: 1, isrs: 1
    partition 8, leader 1, replicas: 1, isrs: 1
    partition 9, leader 1, replicas: 1, isrs: 1
    partition 10, leader 1, replicas: 1, isrs: 1
    partition 11, leader 1, replicas: 1, isrs: 1
    partition 12, leader 1, replicas: 1, isrs: 1
    partition 13, leader 1, replicas: 1, isrs: 1
    partition 14, leader 1, replicas: 1, isrs: 1
    partition 15, leader 1, replicas: 1, isrs: 1
    partition 16, leader 1, replicas: 1, isrs: 1
    partition 17, leader 1, replicas: 1, isrs: 1
    partition 18, leader 1, replicas: 1, isrs: 1
    partition 19, leader 1, replicas: 1, isrs: 1
    partition 20, leader 1, replicas: 1, isrs: 1
    partition 21, leader 1, replicas: 1, isrs: 1
    partition 22, leader 1, replicas: 1, isrs: 1
    partition 23, leader 1, replicas: 1, isrs: 1
    partition 24, leader 1, replicas: 1, isrs: 1
    partition 25, leader 1, replicas: 1, isrs: 1
    partition 26, leader 1, replicas: 1, isrs: 1
    partition 27, leader 1, replicas: 1, isrs: 1
    partition 28, leader 1, replicas: 1, isrs: 1
    partition 29, leader 1, replicas: 1, isrs: 1
    partition 30, leader 1, replicas: 1, isrs: 1
    partition 31, leader 1, replicas: 1, isrs: 1
    partition 32, leader 1, replicas: 1, isrs: 1
    partition 33, leader 1, replicas: 1, isrs: 1
    partition 34, leader 1, replicas: 1, isrs: 1
    partition 35, leader 1, replicas: 1, isrs: 1
    partition 36, leader 1, replicas: 1, isrs: 1
    partition 37, leader 1, replicas: 1, isrs: 1
    partition 38, leader 1, replicas: 1, isrs: 1
    partition 39, leader 1, replicas: 1, isrs: 1
    partition 40, leader 1, replicas: 1, isrs: 1
    partition 41, leader 1, replicas: 1, isrs: 1
    partition 42, leader 1, replicas: 1, isrs: 1
    partition 43, leader 1, replicas: 1, isrs: 1
    partition 44, leader 1, replicas: 1, isrs: 1
    partition 45, leader 1, replicas: 1, isrs: 1
    partition 46, leader 1, replicas: 1, isrs: 1
    partition 47, leader 1, replicas: 1, isrs: 1
    partition 48, leader 1, replicas: 1, isrs: 1
    partition 49, leader 1, replicas: 1, isrs: 1
  topic "MetadataChangeEvent" with 1 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
  topic "__confluent.support.metrics" with 1 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
  topic "MetadataAuditEvent" with 1 partitions:
    partition 0, leader 1, replicas: 1, isrs: 1
```

## How can I check if search indices are created in Elasticsearch?

You can run below command to see the search indices created in your Elasticsearch.

```bash
curl http://localhost:9200/_cat/indices
```

Confirm that `datasetdocument` & `corpuserinfodocument` indices exist besides the default ones. Example response as below:

```bash
yellow open .monitoring-es-6-2020.01.27     hNu-jjU3Tl2SKKFdXzjxHQ 1 1 27279 34  14.8mb  14.8mb
yellow open .watcher-history-6-2020.01.27   70BeSxOkQiCsBCGNtZNfAw 1 1  1210  0     1mb     1mb
yellow open corpuserinfodocument            VCupUjstS4SrZHLDruwVzg 5 1     2  0    11kb    11kb
yellow open .monitoring-kibana-6-2020.01.27 pfJy8HOxRQKG-RQKexMKkA 1 1  1456  0 688.3kb 688.3kb
yellow open .watches                        jmJxYOjrSamqlTi-UIrxTA 1 1     4  0  19.6kb  19.6kb
yellow open datasetdocument                 5HB_IpjYSbOh3QUSUeuwgA 5 1     3  0  27.9kb  27.9kb
yellow open .monitoring-alerts-6            qEAoSNpTRRyqO7fqAzwpeg 1 1     1  0   6.2kb   6.2kb
yellow open .triggered_watches              7g7_MGXFR7mBx0FwQzxpUg 1 1     0  0  48.1kb  48.1kb
yellow open .kibana                         HEQj4GnTQauN3HkwM8CPng 1 1     1  0   3.2kb   3.2kb
```

## How can I check if data has been loaded into MySQL properly?

Once the mysql container is up and running, you should be able to connect to it dirctly on `localhost:3306` using tools such as [MySQL Workbench](https://www.mysql.com/products/workbench/). You can also run the following command to invoke [MySQL Command-Line Client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html) inside the mysql container.

```
docker exec -it mysql /usr/bin/mysql datahub --user=datahub --password=datahub
```

Inspect the content of `metadata_aspect` table, which contains the ingested aspects for all entities. 

## Getting error while starting Docker containers
There can be different reasons why a container fails during initialization. Below are the most common reasons:

### `bind: address already in use`
This error means that the network port (which is supposed to be used by the failed container) is already in use on your system. You need to find and kill the process which is using this specific port before starting the corresponding Docker container. If you don't want to kill the process which is using that port, another option is to change the port number for the Docker container. You need to find and change the [ports](https://docs.docker.com/compose/compose-file/#ports) parameter for the specific Docker container in the `docker-compose.yml` configuration file.

```
Example : On macOS

ERROR: for mysql  Cannot start service mysql: driver failed programming external connectivity on endpoint mysql (5abc99513affe527299514cea433503c6ead9e2423eeb09f127f87e2045db2ca): Error starting userland proxy: listen tcp 0.0.0.0:3306: bind: address already in use

   1) sudo lsof -i :3306
   2) kill -15 <PID found in step1>
``` 
### `OCI runtime create failed`
If you see an error message like below, please make sure to git update your local repo to HEAD.
```
ERROR: for datahub-mae-consumer  Cannot start service datahub-mae-consumer: OCI runtime create failed: container_linux.go:349: starting container process caused "exec: \"bash\": executable file not found in $PATH": unknown
```

### `failed to register layer: devmapper: Unknown device`
This most means that you're out of disk space (see [#1879](https://github.com/linkedin/datahub/issues/1879)).

## toomanyrequests: too many failed login attempts for username or IP address
Try the following
```bash
rm ~/.docker/config.json
docker login
```
More discussions on the same issue https://github.com/docker/hub-feedback/issues/1250

## Seeing `Table 'datahub.metadata_aspect' doesn't exist` error when logging in
This means the database wasn't properly initialized as part of the quickstart processs (see [#1816](https://github.com/linkedin/datahub/issues/1816)). Please run the following command to manually initialize it.
```
docker exec -i mysql sh -c 'exec mysql datahub -udatahub -pdatahub' < docker/mysql/init.sql
```

## I've messed up my docker setup. How do I start from scratch?
1. Delete *all* docker containers, including ones that are created outside of the quickstart guide.
```
docker rm -f $(docker ps -aq)
```
2. Drop all DataHub's docker volumes.
```
docker volume rm -f $(docker volume ls -f name=datahub_  -q)
```
3. Delete DataHub's network
```
docker network rm datahub_network
```
