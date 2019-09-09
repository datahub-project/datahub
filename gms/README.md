# Data Hub Generalized Metadata Store (GMS)

## Starting GMS

```
./gradlew build && ./gradlew :gms:war:JettyRunWar
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