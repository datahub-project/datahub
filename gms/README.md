# DataHub Generalized Metadata Store (GMS)
DataHub GMS is a [Rest.li](https://linkedin.github.io/rest.li/) service written in Java. It is following common 
Rest.li server development practices and all data models are Pegasus(.pdl) models.

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) 
installed on your machine to be able to build `DataHub GMS`.

## Build
`DataHub GMS` is already built as part of top level build:
```
./gradlew build
```
However, if you only want to build `DataHub GMS` specifically:
```
./gradlew :gms:war:build
```

## Dependencies
Before starting `DataHub GMS`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../docker/kafka),  
[Elasticsearch](../docker/elasticsearch) and [MySQL](../docker/mysql) Docker containers are up and running.

## Start via Docker image
Quickest way to try out `DataHub GMS` is running the [Docker image](../docker/gms).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
./gradlew :gms:war:run
```

## API Documentation

You can access basic documentation on the API endpoints by opening the `/restli/docs` endpoint in the browser.
```
python -c "import webbrowser; webbrowser.open('http://localhost:8080/restli/docs', new=2)"
```

## Sample API Calls

### Create user
```
curl 'http://localhost:8080/corpUsers?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"snapshot": {"aspects": [{"com.linkedin.identity.CorpUserInfo":{"active": true, "displayName": "Foo Bar", "fullName": "Foo Bar", "email": "fbar@linkedin.com"}}, {"com.linkedin.identity.CorpUserEditableInfo":{}}], "urn": "urn:li:corpuser:fbar"}}'
```

### Create group
```
curl 'http://localhost:8080/corpGroups?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"snapshot": {"aspects": [{"com.linkedin.identity.CorpGroupInfo":{"email": "dev@linkedin.com", "admins": ["urn:li:corpUser:jdoe"], "members": ["urn:li:corpUser:datahub", "urn:li:corpUser:jdoe"], "groups": []}}], "urn": "urn:li:corpGroup:dev"}}'
```

### Create dataset
```
curl 'http://localhost:8080/datasets?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '{"snapshot": {"aspects":[{"com.linkedin.common.Ownership":{"owners":[{"owner":"urn:li:corpuser:fbar","type":"DATAOWNER"}],"lastModified":{"time":0,"actor":"urn:li:corpuser:fbar"}}},{"com.linkedin.dataset.UpstreamLineage":{"upstreams":[{"auditStamp":{"time":0,"actor":"urn:li:corpuser:fbar"},"dataset":"urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)","type":"TRANSFORMED"}]}},{"com.linkedin.common.InstitutionalMemory":{"elements":[{"url":"https://www.linkedin.com","description":"Sample doc","createStamp":{"time":0,"actor":"urn:li:corpuser:fbar"}}]}},{"com.linkedin.schema.SchemaMetadata":{"schemaName":"FooEvent","platform":"urn:li:dataPlatform:foo","version":0,"created":{"time":0,"actor":"urn:li:corpuser:fbar"},"lastModified":{"time":0,"actor":"urn:li:corpuser:fbar"},"hash":"","platformSchema":{"com.linkedin.schema.KafkaSchema":{"documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"}},"fields":[{"fieldPath":"foo","description":"Bar","nativeDataType":"string","type":{"type":{"com.linkedin.schema.StringType":{}}}}]}}],"urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"}}'
```

### Get user
```
curl 'http://localhost:8080/corpUsers/($params:(),name:fbar)' -H 'X-RestLi-Protocol-Version:2.0.0' -s | jq

{
  "editableInfo": {},
  "username": "fbar",
  "info": {
    "displayName": "Foo Bar",
    "active": true,
    "fullName": "Foo Bar",
    "email": "fbar@linkedin.com"
  }
}
```

### Get group
```
curl 'http://localhost:8080/corpGroups/($params:(),name:dev)' -H 'X-RestLi-Protocol-Version:2.0.0' -s | jq

{
  "name": "dev",
  "info": {
    "groups": [],
    "email": "dev@linkedin.com",
    "admins": [
      "urn:li:corpuser:jdoe"
    ],
    "members": [
      "urn:li:corpuser:datahub",
      "urn:li:corpuser:jdoe"
    ]
  }
}
```

### Get dataset
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:bar,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)' | jq

{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
  "ownership": {
    "owners": [
      {
        "owner": "urn:li:corpuser:fbar",
        "type": "DATAOWNER"
      }
    ],
    "lastModified": {
      "actor": "urn:li:corpuser:fbar",
      "time": 0
    }
  },
  "origin": "PROD",
  "name": "bar",
  "institutionalMemory": {
    "elements": [
      {
        "createStamp": {
          "actor": "urn:li:corpuser:fbar",
          "time": 0
        },
        "description": "Sample doc",
        "url": "https://www.linkedin.com"
      }
    ]
  },
  "schemaMetadata": {
    "created": {
      "actor": "urn:li:corpuser:fbar",
      "time": 0
    },
    "platformSchema": {
      "com.linkedin.schema.KafkaSchema": {
        "documentSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
      }
    },
    "lastModified": {
      "actor": "urn:li:corpuser:fbar",
      "time": 0
    },
    "schemaName": "FooEvent",
    "fields": [
      {
        "fieldPath": "foo",
        "description": "Bar",
        "type": {
          "type": {
            "com.linkedin.schema.StringType": {}
          }
        },
        "nativeDataType": "string"
      }
    ],
    "version": 0,
    "hash": "",
    "platform": "urn:li:dataPlatform:foo"
  },
  "upstreamLineage": {
    "upstreams": [
      {
        "auditStamp": {
          "actor": "urn:li:corpuser:fbar",
          "time": 0
        },
        "type": "TRANSFORMED",
        "dataset": "urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)"
      }
    ]
  },
  "platform": "urn:li:dataPlatform:foo"
}
```

### Get all users
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get_all' 'http://localhost:8080/corpUsers' | jq

{
  "elements": [
    {
      "editableInfo": {},
      "username": "fbar",
      "info": {
        "displayName": "Foo Bar",
        "active": true,
        "fullName": "Foo Bar",
        "email": "fbar@linkedin.com"
      }
    },
    {
      "editableInfo": {
        "skills": [],
        "teams": [],
        "pictureLink": "https://content.linkedin.com/content/dam/me/business/en-us/amp/brand-site/v2/bg/LI-Bug.svg.original.svg"
      },
      "username": "ksahin",
      "info": {
        "displayName": "Kerem Sahin",
        "active": true,
        "fullName": "Kerem Sahin",
        "email": "ksahin@linkedin.com"
      }
    },
    {
      "editableInfo": {
        "skills": [],
        "teams": [],
        "pictureLink": "https://content.linkedin.com/content/dam/me/business/en-us/amp/brand-site/v2/bg/LI-Bug.svg.original.svg"
      },
      "username": "datahub",
      "info": {
        "displayName": "Data Hub",
        "active": true,
        "fullName": "Data Hub",
        "email": "datahub@linkedin.com"
      }
    }
  ],
  "paging": {
    "count": 10,
    "start": 0,
    "links": []
  }
}
```

### Browse datasets
```
curl "http://localhost:8080/datasets?action=browse" -d '{"path": "", "start": 0, "limit": 10}' -X POST -H 'X-RestLi-Protocol-Version: 2.0.0' | jq

{
  "value": {
    "numEntities": 0,
    "metadata": {
      "totalNumEntities": 2,
      "path": "",
      "groups": [
        {
          "name": "prod",
          "count": 2
        }
      ]
    },
    "entities": [],
    "pageSize": 10,
    "from": 0
  }
}
```

### Search users
```
curl "http://localhost:8080/corpUsers?q=search&input=foo&" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq

{
  "metadata": {
    "searchResultMetadatas": [
      {
        "name": "title",
        "aggregations": {}
      }
    ]
  },
  "elements": [
    {
      "editableInfo": {},
      "username": "fbar",
      "info": {
        "displayName": "Foo Bar",
        "active": true,
        "fullName": "Foo Bar",
        "email": "fbar@linkedin.com"
      }
    }
  ],
  "paging": {
    "total": 1,
    "count": 10,
    "start": 0,
    "links": []
  }
}
```

### Search datasets
```
curl "http://localhost:8080/datasets?q=search&input=bar" -X GET -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'X-RestLi-Method: finder' | jq

{
  "metadata": {
    "searchResultMetadatas": [
      {
        "name": "platform",
        "aggregations": {
          "foo": 1
        }
      },
      {
        "name": "origin",
        "aggregations": {
          "prod": 1
        }
      }
    ]
  },
  "elements": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
      "origin": "PROD",
      "name": "bar",
      "platform": "urn:li:dataPlatform:foo"
    }
  ],
  "paging": {
    "total": 1,
    "count": 10,
    "start": 0,
    "links": []
  }
}
```

### Typeahead for datasets
```
curl "http://localhost:8080/datasets?action=autocomplete" -d '{"query": "bar", "field": "name", "limit": 10, "filter": {"criteria": []}}' -X POST -H 'X-RestLi-Protocol-Version: 2.0.0' | jq

{
  "value": {
    "query": "bar",
    "suggestions": [
      "bar"
    ]
  }
}
```

### Get dataset ownership
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:bar,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/rawOwnership/0' | jq

{
  "owners": [
    {
      "owner": "urn:li:corpuser:fbar",
      "type": "DATAOWNER"
    },
    {
      "owner": "urn:li:corpuser:ksahin",
      "type": "DATAOWNER"
    }
  ],
  "lastModified": {
    "actor": "urn:li:corpuser:ksahin",
    "time": 1568015476480
  }
}
```

### Get dataset schema
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:bar,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/schema/0' | jq

{
  "created": {
    "actor": "urn:li:corpuser:fbar",
    "time": 0
  },
  "platformSchema": {
    "com.linkedin.schema.KafkaSchema": {
      "documentSchema": "{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
    }
  },
  "lastModified": {
    "actor": "urn:li:corpuser:fbar",
    "time": 0
  },
  "schemaName": "FooEvent",
  "fields": [
    {
      "fieldPath": "foo",
      "description": "Bar",
      "type": {
        "type": {
          "com.linkedin.schema.StringType": {}
        }
      },
      "nativeDataType": "string"
    }
  ],
  "version": 0,
  "platform": "urn:li:dataPlatform:foo",
  "hash": ""
}
```

### Get upstream datasets
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:bar,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/upstreamLineage/0' | jq

{
  "upstreams": [
    {
      "auditStamp": {
        "actor": "urn:li:corpuser:fbar",
        "time": 0
      },
      "type": "TRANSFORMED",
      "dataset": "urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)"
    }
  ]
}
```

### Get downstream datasets
```
curl -H 'X-RestLi-Protocol-Version:2.0.0' -H 'X-RestLi-Method: get' 'http://localhost:8080/datasets/($params:(),name:barUp,origin:PROD,platform:urn%3Ali%3AdataPlatform%3Afoo)/downstreamLineage' | jq

{
  "downstreams": [
    {
      "type": "TRANSFORMED",
      "auditStamp": {
        "actor": "urn:li:corpuser:fbar",
        "time": 0
      },
      "dataset": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"
    }
  ]
}
```
