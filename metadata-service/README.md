---
title: "metadata-service"
---

# DataHub Metadata Service (Also known as GMS)
DataHub Metadata Service is a service written in Java consisting of multiple servlets: 

1. A public GraphQL API for fetching and mutating objects on the metadata graph. 
2. A general-purpose Rest.li API for ingesting the underlying storage models composing the Metadata graph.

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) 
installed on your machine to be able to build `DataHub Metadata Service`.

## Build
`DataHub Metadata Service` is already built as part of top level build:
```
./gradlew build
```
However, if you only want to build `DataHub Metadata Service` specifically:
```
./gradlew :metadata-service:war:build
```

## Dependencies
Before starting `DataHub Metadata Service`, you need to make sure that [Kafka, Schema Registry & Zookeeper](../docker/kafka-setup),  
[Elasticsearch](../docker/elasticsearch) and [MySQL](../docker/mysql) Docker containers are up and running.

## Start via Docker image
Quickest way to try out `DataHub Metadata Service`` is running the [Docker image](../docker/datahub-gms).

## Start via command line
If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
./gradlew :metadata-service:war:run
```

To run with debug logs printed to console, use

```
./gradlew :metadata-service:war:run -Dlogback.debug=true
```

## API Documentation

The Metadata Service hosts 2 distinct APIs: 

1. [GraphQL](https://graphql.org/) API
2. [Rest.li](https://linkedin.github.io/rest.li/) API

The **GraphQL API** serves as the primary public API for the platform. It can be used to fetch and update metadata programatically in the
language of your choice.

The **Rest.li** API represents the underlying persistence layer, and exposes the raw PDL models used in storage. 
Under the hood, it powers the GraphQL API. Aside from that, it is also used for system-specific ingestion of metadata, being used by
the Metadata Ingestion Framework for pushing metadata into DataHub directly. For all intents and purposes, the Rest.li API is considered system-internal, 
meaning DataHub components are the only ones to consume this API directly. 

When in doubt, opt to build on top of the friendlier GraphQL API. 

### GraphQL API

DataHub Metadata Service exposes a [GraphQL](https://graphql.org/) endpoint serving as the public API for DataHub metadata.

#### Exploring the GraphQL API

To access GraphiQL, a browser-based exploration & documentation tool, simply navigate to either

- `localhost:8080/api/graphiql` (`metadata-service`)
- `localhost:9002/api/graphiql` (`datahub-frontend-react`)

after you've deployed your instance of DataHub.

Note that the version of the API available via the `datahub-frontend-react` container is protected by Cookie-based
Authentication, whereas the API living at the `metadata-service` is not behind any Authentication by default.

In the coming weeks and months, we will be continuing to build out and improve the GraphQL API. Do note that it is still
subject to frequent change, so please plan for this as you build on top of it.

##### Language Support
To issue a GraphQL query programmatically, you can make use of a GraphQL client library. For a full list, see
[GraphQL Code Libraries, Tools, & Services](https://graphql.org/code/).

Questions, concerns, feedback? Something you'd like to see added to the GraphQL API? Let us know on Slack! 

### Rest.li API

You can access basic documentation on the API endpoints by opening the `/restli/docs` endpoint in the browser.
```
python -c "import webbrowser; webbrowser.open('http://localhost:8080/restli/docs', new=2)"
```

*Please note that because DataHub is in a period of rapid development, the APIs below are subject to change. 

#### Sample API Calls

#### Ingesting Aspects

To ingest individual aspects into DataHub, you can use the following CURL:

```shell
curl --location --request POST 'http://localhost:8080/aspects?action=ingestProposal' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
  "proposal" : {
    "entityType": "dataset",
    "entityUrn" : "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
    "changeType" : "UPSERT",
    "aspectName" : "datasetUsageStatistics",
    "aspect" : {
      "value" : "{ \"timestampMillis\":1629840771000,\"uniqueUserCount\" : 10, \"totalSqlQueries\": 20, \"fieldCounts\": [ {\"fieldPath\": \"col1\", \"count\": 20}, {\"fieldPath\" : \"col2\", \"count\": 5} ]}",
      "contentType": "application/json"
    }
  }
}'
```

Notice that you need to provide the target entity urn, the entity type, a change type (`UPSERT` + `DELETE` supported),
the aspect name, and a JSON-serialized aspect, which corresponds to the PDL schema defined for the aspect.

For more examples of serialized aspect payloads, see [bootstrap_mce.json](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/mce_files/bootstrap_mce.json).

#### Ingesting Entities (Legacy)

> Note - we are deprecating support for ingesting Entities via Snapshots. Please see **Ingesting Aspects** above for the latest
> guidance around ingesting metadata into DataHub without defining or changing the legacy snapshot models. (e.g. using ConfigEntityRegistry)

The Entity Snapshot Ingest endpoints allow you to ingest multiple aspects about a particular entity at the same time. 

##### Create a user

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.CorpUserSnapshot":{
            "urn":"urn:li:corpuser:footbarusername",
            "aspects":[
               {
                  "com.linkedin.identity.CorpUserInfo":{
                     "active":true,
                     "displayName":"Foo Bar",
                     "fullName":"Foo Bar",
                     "email":"fbar@linkedin.com"
                  }
               }
            ]
         }
      }
   }
}'
```

##### Create a group

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.CorpGroupSnapshot":{
            "urn":"urn:li:corpGroup:dev",
            "aspects":[
               {
                  "com.linkedin.identity.CorpGroupInfo":{
                     "email":"dev@linkedin.com",
                     "admins":[
                        "urn:li:corpUser:jdoe"
                     ],
                     "members":[
                        "urn:li:corpUser:datahub",
                        "urn:li:corpUser:jdoe"
                     ],
                     "groups":[
                        
                     ]
                  }
               }
            ]
         }
      }
   }
}'
```

##### Create a dataset
```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.DatasetSnapshot":{
            "urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
            "aspects":[
               {
                  "com.linkedin.common.Ownership":{
                     "owners":[
                        {
                           "owner":"urn:li:corpuser:fbar",
                           "type":"DATAOWNER"
                        }
                     ],
                     "lastModified":{
                        "time":0,
                        "actor":"urn:li:corpuser:fbar"
                     }
                  }
               },
               {
                  "com.linkedin.common.InstitutionalMemory":{
                     "elements":[
                        {
                           "url":"https://www.linkedin.com",
                           "description":"Sample doc",
                           "createStamp":{
                              "time":0,
                              "actor":"urn:li:corpuser:fbar"
                           }
                        }
                     ]
                  }
               },
               {
                  "com.linkedin.schema.SchemaMetadata":{
                     "schemaName":"FooEvent",
                     "platform":"urn:li:dataPlatform:foo",
                     "version":0,
                     "created":{
                        "time":0,
                        "actor":"urn:li:corpuser:fbar"
                     },
                     "lastModified":{
                        "time":0,
                        "actor":"urn:li:corpuser:fbar"
                     },
                     "hash":"",
                     "platformSchema":{
                        "com.linkedin.schema.KafkaSchema":{
                           "documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                        }
                     },
                     "fields":[
                        {
                           "fieldPath":"foo",
                           "description":"Bar",
                           "nativeDataType":"string",
                           "type":{
                              "type":{
                                 "com.linkedin.schema.StringType":{
                                    
                                 }
                              }
                           }
                        }
                     ]
                  }
               }
            ]
         }
      }
   }
}'
```

##### Create a chart
```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.ChartSnapshot":{
            "urn":"urn:li:chart:(looker,baz1)",
            "aspects":[
               {
                  "com.linkedin.chart.ChartInfo":{
                     "title":"Baz Chart 1",
                     "description":"Baz Chart 1",
                     "inputs":[
                        {
                           "string":"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
                        }
                     ],
                     "lastModified":{
                        "created":{
                           "time":0,
                           "actor":"urn:li:corpuser:jdoe"
                        },
                        "lastModified":{
                           "time":0,
                           "actor":"urn:li:corpuser:datahub"
                        }
                     }
                  }
               }
            ]
         }
      }
   }
}'
```

##### Create a dashboard
```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.DashboardSnapshot":{
            "urn":"urn:li:dashboard:(looker,baz)",
            "aspects":[
               {
                  "com.linkedin.dashboard.DashboardInfo":{
                     "title":"Baz Dashboard",
                     "description":"Baz Dashboard",
                     "charts":[
                        "urn:li:chart:(looker,baz1)",
                        "urn:li:chart:(looker,baz2)"
                     ],
                     "lastModified":{
                        "created":{
                           "time":0,
                           "actor":"urn:li:corpuser:jdoe"
                        },
                        "lastModified":{
                           "time":0,
                           "actor":"urn:li:corpuser:datahub"
                        }
                     }
                  }
               }
            ]
         }
      }
   }
}'
```

##### Create Tags 

To create a new tag called "Engineering", we can use the following curl. 

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.TagSnapshot":{
            "urn":"urn:li:tag:Engineering",
            "aspects":[
               {
                  "com.linkedin.tag.TagProperties":{
                     "name":"Engineering",
                     "description":"The tag will be assigned to all assets owned by the Eng org."
                  }
               }
            ]
         }
      }
   }
}'
```

This tag can subsequently be associated with a Data Asset using the "Global Tags" aspect associated with each. For example,
to add a tag to a Dataset, you can use the following CURL:

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.DatasetSnapshot":{
            "urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
            "aspects":[
               {
                  "com.linkedin.common.GlobalTags":{
                     "tags":[
                        {
                           "tag":"urn:li:tag:Engineering"
                        }
                     ]
                  }
               }
            ]
         }
      }
   }
}'
```

And to add the tag to a field in a particular Dataset's schema, you can use a CURL to update the EditableSchemaMetadata Aspect:

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.DatasetSnapshot":{
            "urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
            "aspects":[
               {
                  "com.linkedin.schema.EditableSchemaMetadata": { 
                     "editableSchemaFieldInfo":[
                        {
                           "fieldPath":"myFieldName",
                           "globalTags": {
                              "tags":[
                                 {
                                     "tag":"urn:li:tag:Engineering"
                                 }
                              ]
                           }
                        }
                     ]
                  }
               }
            ]
         }
      }
   }
}'
```


##### Soft Deleting an Entity

DataHub uses a special "Status" aspect associated with each entity to represent the lifecycle state of an Entity.
To soft delete an entire Entity, such that it no longer appears in the UI, you can use the special "Status" aspect.

For example, to delete a particular chart:

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.ChartSnapshot":{
            "aspects":[
               {
                  "com.linkedin.common.Status":{
                     "removed": true
                  }
               }
            ],
            "urn":"urn:li:chart:(looker,baz1)"
         }
      }
   }
}'
```

To re-enable the Entity, you can make a similar request:

```
curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
   "entity":{
      "value":{
         "com.linkedin.metadata.snapshot.ChartSnapshot":{
            "aspects":[
               {
                  "com.linkedin.common.Status":{
                     "removed": false
                  }
               }
            ],
            "urn":"urn:li:chart:(looker,baz1)"
         }
      }
   }
}'
```

To issue a hard delete or soft-delete, or undo a particular ingestion run, you can use the [DataHub CLI](../docs/how/delete-metadata.md). 


#### Retrieving Entity Aspects

Simply curl the `entitiesV2` endpoint of GMS:

```
curl  'http://localhost:8080/entitiesV2/<url-encoded-entity-urn>'
```

For example, to retrieve the latest aspects associated with the "SampleHdfsDataset" `Dataset`: 

```
curl --header 'X-RestLi-Protocol-Version: 2.0.0' 'http://localhost:8080/entitiesV2/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahdfs%2CSampleHdfsDataset%2CPROD%29'
```

**Example Response**

```json
{
   "urn":"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
   "aspects":{
      "editableSchemaMetadata":{
         "name":"editableSchemaMetadata",
         "version":0,
         "value":{
            "created":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            },
            "editableSchemaFieldInfo":[
               {
                  "fieldPath":"shipment_info",
                  "globalTags":{
                     "tags":[
                        {
                           "tag":"urn:li:tag:Legacy"
                        }
                     ]
                  }
               }
            ],
            "lastModified":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            }
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "browsePaths":{
         "name":"browsePaths",
         "version":0,
         "value":{
            "paths":[
               "/prod/hdfs/SampleHdfsDataset"
            ]
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "datasetKey":{
         "name":"datasetKey",
         "version":0,
         "value":{
            "name":"SampleHdfsDataset",
            "platform":"urn:li:dataPlatform:hdfs",
            "origin":"PROD"
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "ownership":{
         "name":"ownership",
         "version":0,
         "value":{
            "owners":[
               {
                  "owner":"urn:li:corpuser:jdoe",
                  "type":"DATAOWNER"
               },
               {
                  "owner":"urn:li:corpuser:datahub",
                  "type":"DATAOWNER"
               }
            ],
            "lastModified":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            }
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "dataPlatformInstance":{
         "name":"dataPlatformInstance",
         "version":0,
         "value":{
            "platform":"urn:li:dataPlatform:hdfs"
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "institutionalMemory":{
         "name":"institutionalMemory",
         "version":0,
         "value":{
            "elements":[
               {
                  "createStamp":{
                     "actor":"urn:li:corpuser:jdoe",
                     "time":1581407189000
                  },
                  "description":"Sample doc",
                  "url":"https://www.linkedin.com"
               }
            ]
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "schemaMetadata":{
         "name":"schemaMetadata",
         "version":0,
         "value":{
            "created":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            },
            "platformSchema":{
               "com.linkedin.schema.KafkaSchema":{
                  "documentSchema":"{\"type\":\"record\",\"name\":\"SampleHdfsSchema\",\"namespace\":\"com.linkedin.dataset\",\"doc\":\"Sample HDFS dataset\",\"fields\":[{\"name\":\"field_foo\",\"type\":[\"string\"]},{\"name\":\"field_bar\",\"type\":[\"boolean\"]}]}"
               }
            },
            "lastModified":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            },
            "schemaName":"SampleHdfsSchema",
            "fields":[
               {
                  "nullable":false,
                  "fieldPath":"shipment_info",
                  "description":"Shipment info description",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.RecordType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"varchar(100)",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.date",
                  "description":"Shipment info date description",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.DateType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"Date",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.target",
                  "description":"Shipment info target description",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.StringType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"text",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.destination",
                  "description":"Shipment info destination description",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.StringType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"varchar(100)",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.geo_info",
                  "description":"Shipment info geo_info description",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.RecordType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"varchar(100)",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.geo_info.lat",
                  "description":"Shipment info geo_info lat",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.NumberType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"float",
                  "recursive":false
               },
               {
                  "nullable":false,
                  "fieldPath":"shipment_info.geo_info.lng",
                  "description":"Shipment info geo_info lng",
                  "isPartOfKey":false,
                  "type":{
                     "type":{
                        "com.linkedin.schema.NumberType":{
                           
                        }
                     }
                  },
                  "nativeDataType":"float",
                  "recursive":false
               }
            ],
            "version":0,
            "hash":"",
            "platform":"urn:li:dataPlatform:hdfs"
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      },
      "upstreamLineage":{
         "name":"upstreamLineage",
         "version":0,
         "value":{
            "upstreams":[
               {
                  "auditStamp":{
                     "actor":"urn:li:corpuser:jdoe",
                     "time":1581407189000
                  },
                  "type":"TRANSFORMED",
                  "dataset":"urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"
               }
            ]
         },
         "created":{
            "actor":"urn:li:corpuser:UNKNOWN",
            "time":1646245614843
         }
      }
   },
   "entityName":"dataset"
}
```

You can also optionally limit to specific aspects using the `aspects` query parameter:

```
curl  'http://localhost:8080/entitiesV2/<url-encoded-entity-urn>?aspects=List(upstreamLineage)'
```

#### Retrieving Entities (Legacy)

> Note that this method of retrieving entities is deprecated, as it uses the legacy Snapshot models. Please refer to the **Retriving Entity Aspects** section above for the
> latest guidance. 

The Entity Snapshot Get APIs allow to retrieve the latest version of each aspect associated with an Entity. 

In general, when reading entities by primary key (urn), you will use the general-purpose `entities` endpoints. To fetch by primary key (urn), you'll
issue a query of the following form:

```
curl  'http://localhost:8080/entities/<url-encoded-entity-urn>'
```

##### Get a CorpUser

```
curl 'http://localhost:8080/entities/urn%3Ali%3Acorpuser%3Afbar'

{
   "value":{
      "com.linkedin.metadata.snapshot.CorpUserSnapshot":{
         "urn":"urn:li:corpuser:fbar",
         "aspects":[
            {
               "com.linkedin.metadata.key.CorpUserKey":{
                  "username":"fbar"
               }
            },
            {
               "com.linkedin.identity.CorpUserInfo":{
                  "active":true,
                  "fullName":"Foo Bar",
                  "displayName":"Foo Bar",
                  "email":"fbar@linkedin.com"
               }
            },
            {
               "com.linkedin.identity.CorpUserEditableInfo":{
                  
               }
            }
         ]
      }
   }
}
```


##### Get a CorpGroup

```
curl 'http://localhost:8080/entities/urn%3Ali%3AcorpGroup%3Adev'

{
   "value":{
      "com.linkedin.metadata.snapshot.CorpGroupSnapshot":{
         "urn":"urn:li:corpGroup:dev",
         "aspects":[
            {
               "com.linkedin.metadata.key.CorpGroupKey":{
                  "name":"dev"
               }
            },
            {
               "com.linkedin.identity.CorpGroupInfo":{
                  "groups":[
                     
                  ],
                  "email":"dev@linkedin.com",
                  "admins":[
                     "urn:li:corpUser:jdoe"
                  ],
                  "members":[
                     "urn:li:corpUser:datahub",
                     "urn:li:corpUser:jdoe"
                  ]
               }
            }
         ]
      }
   }
}
```

##### Get a Dataset
```
curl 'http://localhost:8080/entities/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Afoo,bar,PROD)'

{
   "value":{
      "com.linkedin.metadata.snapshot.DatasetSnapshot":{
         "urn":"urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
         "aspects":[
            {
               "com.linkedin.metadata.key.DatasetKey":{
                  "origin":"PROD",
                  "name":"bar",
                  "platform":"urn:li:dataPlatform:foo"
               }
            },
            {
               "com.linkedin.common.InstitutionalMemory":{
                  "elements":[
                     {
                        "createStamp":{
                           "actor":"urn:li:corpuser:fbar",
                           "time":0
                        },
                        "description":"Sample doc",
                        "url":"https://www.linkedin.com"
                     }
                  ]
               }
            },
            {
               "com.linkedin.common.Ownership":{
                  "owners":[
                     {
                        "owner":"urn:li:corpuser:fbar",
                        "type":"DATAOWNER"
                     }
                  ],
                  "lastModified":{
                     "actor":"urn:li:corpuser:fbar",
                     "time":0
                  }
               }
            },
            {
               "com.linkedin.schema.SchemaMetadata":{
                  "created":{
                     "actor":"urn:li:corpuser:fbar",
                     "time":0
                  },
                  "platformSchema":{
                     "com.linkedin.schema.KafkaSchema":{
                        "documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
                     }
                  },
                  "lastModified":{
                     "actor":"urn:li:corpuser:fbar",
                     "time":0
                  },
                  "schemaName":"FooEvent",
                  "fields":[
                     {
                        "fieldPath":"foo",
                        "description":"Bar",
                        "type":{
                           "type":{
                              "com.linkedin.schema.StringType":{
                                 
                              }
                           }
                        },
                        "nativeDataType":"string"
                     }
                  ],
                  "version":0,
                  "hash":"",
                  "platform":"urn:li:dataPlatform:foo"
               }
            },
            {
               "com.linkedin.common.BrowsePaths":{
                  "paths":[
                     "/prod/foo/bar"
                  ]
               }
            },
            {
               "com.linkedin.dataset.UpstreamLineage":{
                  "upstreams":[
                     {
                        "auditStamp":{
                           "actor":"urn:li:corpuser:fbar",
                           "time":0
                        },
                        "type":"TRANSFORMED",
                        "dataset":"urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)"
                     }
                  ]
               }
            }
         ]
      }
   }
}
```

##### Get a Chart
```
curl 'http://localhost:8080/entities/urn%3Ali%3Achart%3A(looker,baz1)'

{
   "value":{
      "com.linkedin.metadata.snapshot.ChartSnapshot":{
         "urn":"urn:li:chart:(looker,baz1)",
         "aspects":[
            {
               "com.linkedin.metadata.key.ChartKey":{
                  "chartId":"baz1",
                  "dashboardTool":"looker"
               }
            },
            {
               "com.linkedin.common.BrowsePaths":{
                  "paths":[
                     "/looker/baz1"
                  ]
               }
            },
            {
               "com.linkedin.chart.ChartInfo":{
                  "description":"Baz Chart 1",
                  "lastModified":{
                     "created":{
                        "actor":"urn:li:corpuser:jdoe",
                        "time":0
                     },
                     "lastModified":{
                        "actor":"urn:li:corpuser:datahub",
                        "time":0
                     }
                  },
                  "title":"Baz Chart 1",
                  "inputs":[
                     {
                        "string":"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
                     }
                  ]
               }
            }
         ]
      }
   }
}
```

##### Get a Dashboard
```
curl 'http://localhost:8080/entities/urn%3Ali%3Adashboard%3A(looker,foo)'

{
   "value":{
      "com.linkedin.metadata.snapshot.DashboardSnapshot":{
         "urn":"urn:li:dashboard:(looker,foo)",
         "aspects":[
            {
               "com.linkedin.metadata.key.DashboardKey":{
                  "dashboardId":"foo",
                  "dashboardTool":"looker"
               }
            }
         ]
      }
   }
}
```

##### Get a GlossaryTerm
```
curl 'http://localhost:8080/entities/urn%3Ali%3AglossaryTerm%3A(instruments,instruments.FinancialInstrument_v1)'
{
   "value":{
      "com.linkedin.metadata.snapshot.GlossaryTermSnapshot":{
         "urn":"urn:li:glossaryTerm:instruments.FinancialInstrument_v1",
         "ownership":{
            "owners":[
               {
                  "owner":"urn:li:corpuser:jdoe",
                  "type":"DATAOWNER"
               }
            ],
            "lastModified":{
               "actor":"urn:li:corpuser:jdoe",
               "time":1581407189000
            }
         },
         "glossaryTermInfo":{
            "definition":"written contract that gives rise to both a financial asset of one entity and a financial liability of another entity",
            "customProperties":{
               "FQDN":"full"
            },
            "sourceRef":"FIBO",
            "sourceUrl":"https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstruments/FinancialInstruments/FinancialInstrument",
            "termSource":"EXTERNAL"
         }
      }
   }
}
```

##### Browse an Entity

To browse (explore) for an Entity of a particular type (e.g. dataset, chart, etc), you can use the following query format:

```
curl -X POST 'http://localhost:8080/entities?action=browse' \
--data '{
    "path": "<slash-delimited-browse-path>",
    "entity": "<entity name>",
    "start": 0,
    "limit": 10
}'
```

For example, to browse the "charts" entity, you could use the following query: 

```
curl -X POST 'http://localhost:8080/entities?action=browse' \
--data '{
    "path": "/looker",
    "entity": "chart",
    "start": 0,
    "limit": 10
}'

{
   "value":{
      "numEntities":1,
      "pageSize":1,
      "metadata":{
         "totalNumEntities":1,
         "groups":[
            
         ],
         "path":"/looker"
      },
      "from":0,
      "entities":[
         {
            "name":"baz1",
            "urn":"urn:li:chart:(looker,baz1)"
         }
      ]
   }
}
```

##### Search an Entity

To search for an Entity of a particular type (e.g. dataset, chart, etc), you can use the following query format: 

```
curl -X POST 'http://localhost:8080/entities?action=search' \
--data '{
    "input": "<query-text>",
    "entity": "<entity name>",
    "start": 0,
    "count": 10
}'
```

The API will return a list of URNs that matched your search query.

For example, to search the "charts" entity, you could use the following query:

```
curl -X POST 'http://localhost:8080/entities?action=search' \
--data '{
    "input": "looker",
    "entity": "chart",
    "start": 0,
    "count": 10
}'

{
   "value":{
      "numEntities":1,
      "pageSize":10,
      "metadata":{
         "urns":[
            "urn:li:chart:(looker,baz1)"
         ],
         "matches":[
            {
               "matchedFields":[
                  {
                     "name":"tool",
                     "value":"looker"
                  }
               ]
            }
         ],
         "searchResultMetadatas":[
            {
               "name":"tool",
               "aggregations":{
                  "looker":1
               }
            }
         ]
      },
      "from":0,
      "entities":[
         "urn:li:chart:(looker,baz1)"
      ]
   }
}
```

###### Exact Match Search

You can use colon search for exact match searching on particular @Searchable fields of an Entity. 

###### Example: Find assets by Tag 

For example, to fetch all Datasets having a particular tag (Engineering), we can use the following query:

```
curl -X POST 'http://localhost:8080/entities?action=search' \
--data '{
    "input": "tags:Engineering",
    "entity": "dataset",
    "start": 0,
    "count": 10
}'

{
   "value":{
      "numEntities":1,
      "pageSize":10,
      "metadata":{
         "urns":[
            "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"
         ],
         "matches":[
            {
               "matchedFields":[
                  {
                     "name":"tags",
                     "value":"urn:li:tag:Engineering"
                  }
               ]
            }
         ],
         "searchResultMetadatas":[
            {
               "name":"platform",
               "aggregations":{
                  "foo":1
               }
            },
            {
               "name":"origin",
               "aggregations":{
                  "PROD":1
               }
            }
         ]
      },
      "from":0,
      "entities":[
         "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)"
      ]
   }
}
```

###### Filtering

In addition to performing full-text search, you can also filter explicitly against fields marked as @Searchable in the corresponding aspect PDLs.

For example, to perform filtering for a chart with title "Baz Chart 1", you could issue the following query:

```
curl -X POST 'http://localhost:8080/entities?action=search' \
--data '{
    "input": "looker",
    "entity": "chart",
    "start": 0,
    "count": 10,
    "filter": {
        "or": [{
            "and": [
               {
                  "field": "title",
                  "value": "Baz Chart 1",
                  "condition": "EQUAL"
               }
            ]   
        }]
    }
}'

{
   "value":{
      "numEntities":1,
      "pageSize":10,
      "metadata":{
         "urns":[
            "urn:li:chart:(looker,baz1)"
         ],
         "matches":[
            {
               "matchedFields":[
                  {
                     "name":"tool",
                     "value":"looker"
                  }
               ]
            }
         ],
         "searchResultMetadatas":[
            {
               "name":"tool",
               "aggregations":{
                  "looker":1
               }
            }
         ]
      },
      "from":0,
      "entities":[
         "urn:li:chart:(looker,baz1)"
      ]
   }
}
```

where valid conditions include 
    - CONTAIN
    - END_WITH
    - EQUAL
    - GREATER_THAN
    - GREATER_THAN_OR_EQUAL_TO
    - LESS_THAN
    - LESS_THAN_OR_EQUAL_TO
    - START_WITH

*Note that the search API only includes data corresponding to the latest snapshots of a particular Entity.


##### Autocomplete against fields of an entity 

To autocomplete a query for a particular entity type, you can use a query of the following form: 

```
curl -X POST 'http://localhost:8080/entities?action=autocomplete' \
--data '{
    "query": "<query-text>",
    "entity": "<entity-name>",
    "limit": 10
}'
```

For example, to autocomplete a query against all Dataset entities, you could issue the following:

```
curl -X POST 'http://localhost:8080/entities?action=autocomplete' \
--data '{
    "query": "Baz Ch",
    "entity": "chart",
    "start": 0,
    "limit": 10
}'

{
   "value":{
      "suggestions":[
         "Baz Chart 1"
      ],
      "query":"Baz Ch"
   }
}
```

Note that you can also provide a `Filter` to the autocomplete endpoint: 

```
curl -X POST 'http://localhost:8080/entities?action=autocomplete' \
--data '{
    "query": "Baz C",
    "entity": "chart",
    "start": 0,
    "limit": 10,
    "filter": {
        "or": [{
            "and": [
               {
                "field": "tool",
                "value": "looker",
                "condition": "EQUAL"
               }
            ]   
        }]
    }
}'

{
   "value":{
      "suggestions":[
         "Baz Chart 1"
      ],
      "query":"Baz Ch"
   }
}
```

*Note that the autocomplete API only includes data corresponding to the latest snapshots of a particular Entity.


##### Get a Versioned Aspect

In addition to fetching the set of latest Snapshot aspects for an entity, we also support doing a point lookup of an entity at a particular version.

To do so, you can use the following query template:

```
curl 'http://localhost:8080/aspects/<url-encoded-entity-urn>?aspect=<aspect-name>&version=<version>
```

Which will return a VersionedAspect, which is a record containing a version and an aspect inside a Rest.li Union, wherein the fully-qualified record name of the
aspect is the key for the union.

For example, to fetch the latest version of a Dataset's "schemaMetadata" aspect, you could issue the following query:

```
curl 'http://localhost:8080/aspects/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Afoo%2Cbar%2CPROD)?aspect=schemaMetadata&version=0'

{
   "version":0,
   "aspect":{
      "com.linkedin.schema.SchemaMetadata":{
         "created":{
            "actor":"urn:li:corpuser:fbar",
            "time":0
         },
         "platformSchema":{
            "com.linkedin.schema.KafkaSchema":{
               "documentSchema":"{\"type\":\"record\",\"name\":\"MetadataChangeEvent\",\"namespace\":\"com.linkedin.mxe\",\"doc\":\"Kafka event for proposing a metadata change for an entity.\",\"fields\":[{\"name\":\"auditHeader\",\"type\":{\"type\":\"record\",\"name\":\"KafkaAuditHeader\",\"namespace\":\"com.linkedin.avro2pegasus.events\",\"doc\":\"Header\"}}]}"
            }
         },
         "lastModified":{
            "actor":"urn:li:corpuser:fbar",
            "time":0
         },
         "schemaName":"FooEvent",
         "fields":[
            {
               "fieldPath":"foo",
               "description":"Bar",
               "type":{
                  "type":{
                     "com.linkedin.schema.StringType":{
                        
                     }
                  }
               },
               "nativeDataType":"string"
            }
         ],
         "version":0,
         "hash":"",
         "platform":"urn:li:dataPlatform:foo"
      }
   }
}
```

Keep in mind that versions increase monotonically *after* version 0, which represents the latest. 

Note that this API will soon be deprecated and replaced by the V2 Aspect API, discussed below. 

##### Get a range of Versioned Aspects

*Coming Soon*! 

##### Get a range of Timeseries Aspects 

With the introduction of Timeseries Aspects, we've introduced a new API for fetching a series of aspects falling into a particular time range. For this, you'll
use the `/aspects` endpoint. The V2 APIs are unique in that they return a new type of payload: an "Enveloped Aspect". This is essentially a serialized aspect along with
some system metadata. The serialized aspect can be in any form, though we currently default to escaped Rest.li-compatible JSON. 

Callers of the V2 Aspect APIs will be expected to deserialize the aspect payload in the way they see fit. For example, they may bind the deserialized JSON object
into a strongly typed Rest.li RecordTemplate class (which is what datahub-frontend does). The benefit of doing it this way is thaet we remove the necessity to 
use Rest.li Unions to represent an object which can take on multiple payload forms. It also makes adding and removing aspects from the model easier, a process
which could theoretically be done at runtime as opposed to at deploy time.

To fetch a set of Timeseries Aspects that fall into a particular time range, you can use the following query template:

```
curl -X POST 'http://localhost:8080/aspects?action=getTimeseriesAspectValues' \
--data '{
    "urn": "<urn>",
    "entity": "<entity-name>",
    "aspect": "<time-series-aspect-name>",
    "startTimeMillis": "<your-start-time-ms>",
    "endTimeMillis": "<your-end-time-ms>"
}'
```

For example, to fetch "datasetProfile" timeseries aspects for a dataset with urn `urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)`
that were reported after July 26, 2021 and before July 28, 2021, you could issue the following query:

```
curl -X POST 'http://localhost:8080/aspects?action=getTimeseriesAspectValues' \
--data '{
    "urn": "urn:li:dataset:(urn:li:dataPlatform:redshift,global_dev.larxynx_carcinoma_data_2020,PROD)",
    "entity": "dataset",
    "aspect": "datasetProfile",
    "startTimeMillis": 1625122800000,
    "endTimeMillis": 1627455600000
}'

{
   "value":{
      "limit":10000,
      "aspectName":"datasetProfile",
      "endTimeMillis":1627455600000,
      "startTimeMillis":1625122800000,
      "entityName":"dataset",
      "values":[
         {
            "aspect":{
               "value":"{\"timestampMillis\":1626912000000,\"fieldProfiles\":[{\"uniqueProportion\":1.0,\"sampleValues\":[\"123MMKK12\",\"13KDFMKML\",\"123NNJJJL\"],\"fieldPath\":\"id\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":1.0,\"min\":\"1524406400000\",\"max\":\"1624406400000\",\"sampleValues\":[\"1640023230002\",\"1640343012207\",\"16303412330117\"],\"mean\":\"1555406400000\",\"fieldPath\":\"date\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":3742},{\"uniqueProportion\":0.037,\"min\":\"21\",\"median\":\"68\",\"max\":\"92\",\"sampleValues\":[\"45\",\"65\",\"81\"],\"mean\":\"65\",\"distinctValueFrequencies\":[{\"value\":\"12\",\"frequency\":103},{\"value\":\"54\",\"frequency\":12}],\"fieldPath\":\"patient_age\",\"nullCount\":0,\"nullProportion\":0.0,\"uniqueCount\":79},{\"uniqueProportion\":0.00820873786407767,\"sampleValues\":[\"male\",\"female\"],\"fieldPath\":\"patient_gender\",\"nullCount\":120,\"nullProportion\":0.03,\"uniqueCount\":2}],\"rowCount\":3742,\"columnCount\":4}",
               "contentType":"application/json"
            }
         },
      ]
   }
}
```

You'll notice that in this API (V2), we return a generic serialized aspect string as opposed to an inlined Rest.li-serialized Snapshot Model.

This is part of an initiative to move from MCE + MAE to MetadataChangeProposal and MetadataChangeLog. For more information, see [this doc](../docs/advanced/mcp-mcl.md). 

##### Get Relationships (Edges)

To get relationships between entities, you can use the `/relationships` API. Do do so, you must provide the following inputs:

1. Urn of the source node 
2. Direction of the edge (INCOMING, OUTGOING)
3. The name of the Relationship (This can be found in Aspect PDLs within the @Relationship annotation)

For example, to get all entities owned by `urn:li:corpuser:fbar`, we could issue the following query: 

```
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Acorpuser%3Auser1&types=OwnedBy'
```

which will return a list of urns, representing entities on the other side of the relationship: 

```
{
   "entities":[
      urn:li:dataset:(urn:li:dataPlatform:foo,barUp,PROD)
   ]
}
```

# FAQ

*1. How do I find the valid set of Entity names?*

Entities are named inside of PDL schemas. Each entity will be annotated with the @Entity annotation, which will include a "name" field inside. 
This represents the "common name" for the entity which can be used in browsing, searching, and more. By default, DataHub ships with the following entities:

By convention, all entity PDLs live under `metadata-models/src/main/pegasus/com/linkedin/metadata/snapshot`

*2. How do I find the valid set of Aspect names?*

Aspects are named inside of PDL schemas. Each aspect will be annotated with the @Aspect annotation, which will include a "name" field inside.
This represents the "common name" for the entity which can be used in browsing, searching, and more.

By convention, all entity PDLs live under `metadata-models/src/main/pegasus/com/linkedin/metadata/common` or `metadata-models/src/main/pegasus/com/linkedin/metadata/<entity-name>`. For example,
the dataset-specific aspects are located under `metadata-models/src/main/pegasus/com/linkedin/metadata/dataset`.

*3. How do I find the valid set of Relationship names?*

All relationships are defined on foreign-key fields inside Aspect PDLs. They are reflected by fields bearing the @Relationship annotation. Inside this annotation
is a "name" field that defines the standardized name of the Relationship to be used when querying. 

By convention, all entity PDLs live under `metadata-models/src/main/pegasus/com/linkedin/metadata/common` or `metadata-models/src/main/pegasus/com/linkedin/metadata/<entity-name>`. For example,
the dataset-specific aspects are located under `metadata-models/src/main/pegasus/com/linkedin/metadata/dataset`.

