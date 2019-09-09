# MetadataAuditEvent (MAE) Consumer Job

## Starting job
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