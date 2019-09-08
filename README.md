# Data Hub
[![Build Status](https://travis-ci.org/linkedin/WhereHows.svg?branch=datahub)](https://travis-ci.org/linkedin/WhereHows)
[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/linkedin/datahub)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/keremsahin/datahub-gms)](https://cloud.docker.com/repository/docker/keremsahin/datahub-gms/)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/keremsahin/datahub-frontend)](https://cloud.docker.com/repository/docker/keremsahin/datahub-frontend/)

## Pre-requisites
Be sure to have JDK installed on your machine.

```
sudo yum install java-1.8.0-openjdk-devel
```

Install docker and docker-compose.
```
Check https://www.docker.com/get-started for instructions on how to install docker-ce
```

Install Chrome web browser.
```
https://www.google.com/chrome/
```

## Quickstart
To start all Docker images at once, please follow below instructions.

```
cd docker/quickstart
docker-compose up
cd ../elasticsearch && bash init.sh
```

## Starting Kafka
Kafka, ZooKeeper and Schema Registry are running in individual Docker containers.
We are using Confluent images. Default configurations are used.

```
cd docker/kafka
docker-compose up
```

## Starting MySQL
MySQL Server runs in its own Docker container. Please run below commands to start MySQL container.
```
cd docker/mysql
docker-compose up
```
To connect to MySQL server you can use below command:
```
docker exec -it mysql mysql -u datahub -pdatahub datahub
```

## Starting ElasticSearch and Kibana
ElasticSearch and Kibana run in their own Docker containers. Please run below commands to start ElasticSearch and Kibana containers.
```
cd docker/elasticsearch
docker-compose up
```
After containers are initialized, we need to create the search index by running below command:
```
bash init.sh
```
You can connect to Kibana on your web browser via below link
```
http://localhost:5601
```

## Starting GMS

```
./gradlew build
./gradlew :gms:war:JettyRunWar
```

### Example GMS Curl Calls

#### Create
```
curl 'http://localhost:8080/corpUsers/($params:(),name:fbar)/snapshot' -X POST -H 'X-RestLi-Method: create' -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"aspects": [{"com.linkedin.identity.CorpUserInfo":{"active": true, "fullName": "Foo Bar", "email": "fbar@linkedin.com"}}, {"com.linkedin.identity.CorpUserEditableInfo":{}}], "urn": "urn:li:corpuser:fbar"}' -v
curl 'http://localhost:8080/datasets/($params:(),name:x.y,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/snapshot' -X POST -H 'X-RestLi-Method: create' -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"aspects":[{"com.linkedin.common.Ownership":{"owners":[{"owner":"urn:li:corpuser:ksahin","type":"DATAOWNER"}],"lastModified":{"time":0,"actor":"urn:li:corpuser:ksahin"}}},{"com.linkedin.dataset.UpstreamLineage":{"upstreams":[{"auditStamp":{"time":0,"actor":"urn:li:corpuser:ksahin"},"dataset":"urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)","type":"TRANSFORMED"}]}},{"com.linkedin.common.InstitutionalMemory":{"elements":[{"url":"https://www.linkedin.com","description":"Sample doc","createStamp":{"time":0,"actor":"urn:li:corpuser:ksahin"}}]}},{"com.linkedin.schema.SchemaMetadata":{"schemaName":"FooEvent","platform":"urn:li:dataPlatform:foo","version":0,"created":{"time":0,"actor":"urn:li:corpuser:ksahin"},"lastModified":{"time":0,"actor":"urn:li:corpuser:ksahin"},"hash":"","platformSchema":{"com.linkedin.schema.KafkaSchema":{"documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"}},"fields":[{"fieldPath":"foo","description":"Bar","nativeDataType":"string","type":{"type":{"com.linkedin.schema.StringType":{}}}}]}}],"urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"}' -v
```

#### Get
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/corpUsers/($params:(),name:fbar)/snapshot/($params:(),aspectVersions:List((aspect:com.linkedin.identity.CorpUserInfo,version:0)))' | jq
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:x.y,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/snapshot/($params:(),aspectVersions:List((aspect:com.linkedin.common.Ownership,version:0)))' | jq
```

### Get all
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get_all' 'http://localhost:8080/corpUsers' | jq
```

### Browse

```
curl "http://localhost:8080/datasets?action=browse" -d '{"path": "", "start": 0, "limit": 10}' -X POST -H 'X-RestLi-Protocol-Version: 2.0.0' | jq
```

### Search

```
curl "http://localhost:8080/corpUsers?q=search&input=foo&" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq
curl "http://localhost:8080/datasets?q=search&input=foo&" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq
```

### Autocomplete

```
curl "http://localhost:8080/datasets?action=autocomplete" -d '{"query": "foo", "field": "name", "limit": 10}' -X POST -H 'X-RestLi-Protocol-Version: 2.0.0' | jq
```

### Ownership

```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:x.y,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/rawOwnership/0' | jq
```

### Schema

```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:x.y,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/schema/0' | jq
```

## Debugging Kafka
GMS fires a MetadataAuditEvent after a new record is created through snapshot endpoint. We can check if this message is correctly fired using kafkacat.
```
Install kafkacat through this link https://github.com/edenhill/kafkacat
```
To consume messages on MetadataAuditEvent topic, run below command. It doesn't support Avro deserialization just yet, but they have an ongoing [work](https://github.com/edenhill/kafkacat/pull/151) for that.
```
kafkacat -b localhost:9092 -t MetadataAuditEvent
```

## Starting Elasticsearch Indexing Job
Run below to start Elasticsearch indexing job.
```
./gradlew :metadata-jobs:elasticsearch-index-job:run
```
To test the job, you should've already started Kafka, GMS, MySQL and ElasticSearch/Kibana.
After starting all the services, you can create a record in GMS by Snapshot endpoint as below.
```
curl 'http://localhost:8080/metrics/($params:(),name:a.b.c01,type:UMP)/snapshot' -X POST -H 'X-RestLi-Method: create' -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"aspects": [{"com.linkedin.common.Ownership":{"owners":[{"owner":"urn:li:corpuser:ksahin","type":"DATAOWNER"}]}}], "urn": "urn:li:metric:(UMP,a.b.c01)"}' -v
```
This will fire an MAE and search index will be updated by indexing job after reading MAE from Kafka.
Then, you can check ES index if document is populated by below command.
```
curl localhost:9200/metricdocument/_search -d '{"query":{"match":{"urn":"urn:li:metric:(UMP,a.b.c01)"}}}' | jq
```

## Starting MetadataChangeEvent Consuming Job
Run below to start MCE consuming job.
```
./gradlew :metadata-jobs:mce-consumer-job:run
```
Create your own MCE to align the models in bootstrap_mce.dat.
Tips: one line per MCE with Python syntax.

Then you can produce MCE to feed your GMS.
```
cd metadata-ingestion && python mce_cli.py produce
```

## Starting Datahub Frontend
Run below to start datahub-frontend Play server.
```
cd datahub-frontend/run
./run-local-frontend
```
Then you can connect to Datahub on your web browser via below link
```
http://localhost:9001
```
