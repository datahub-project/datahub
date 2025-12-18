---
sidebar_position: 5
title: DataProcessInstance
slug: /generated/metamodel/entities/dataprocessinstance
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataProcessInstance.md
---
# DataProcessInstance
DataProcessInstance represents an instance of a datajob/jobflow run
## Aspects

### dataProcessInstanceInput
Information about the inputs datasets of a Data process
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceInput"
  },
  "name": "DataProcessInstanceInput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset",
            "mlModel"
          ],
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "inputs",
          "fieldType": "URN",
          "numValuesFieldName": "numInputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "inputs",
      "doc": "Input assets consumed"
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputEdges/*/created/actor",
          "createdOn": "inputEdges/*/created/time",
          "entityTypes": [
            "dataset",
            "mlModel",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "name": "DataProcessInstanceConsumes",
          "properties": "inputEdges/*/properties",
          "updatedActor": "inputEdges/*/lastModified/actor",
          "updatedOn": "inputEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Edge",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "sourceUrn",
                "default": null,
                "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "destinationUrn",
                "doc": "Urn of the destination of this relationship edge."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "AuditStamp",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "impersonator",
                        "default": null,
                        "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "message",
                        "default": null,
                        "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                      }
                    ],
                    "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                  }
                ],
                "name": "created",
                "default": null,
                "doc": "Audit stamp containing who created this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  "com.linkedin.common.AuditStamp"
                ],
                "name": "lastModified",
                "default": null,
                "doc": "Audit stamp containing who last modified this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "map",
                    "values": "string"
                  }
                ],
                "name": "properties",
                "default": null,
                "doc": "A generic properties bag that allows us to store specific information on this graph edge."
              }
            ],
            "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
          }
        }
      ],
      "name": "inputEdges",
      "default": null,
      "doc": "Input assets consumed by the data process instance, with additional metadata.\nCounts as lineage.\nWill eventually deprecate the inputs field."
    }
  ],
  "doc": "Information about the inputs datasets of a Data process"
}
```
</details>

### dataProcessInstanceOutput
Information about the outputs of a Data process
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceOutput"
  },
  "name": "DataProcessInstanceOutput",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset",
            "mlModel"
          ],
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "outputs",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputs",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "outputs",
      "doc": "Output assets produced"
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "outputEdges/*/created/actor",
          "createdOn": "outputEdges/*/created/time",
          "entityTypes": [
            "dataset",
            "mlModel",
            "dataProcessInstance"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "DataProcessInstanceProduces",
          "properties": "outputEdges/*/properties",
          "updatedActor": "outputEdges/*/lastModified/actor",
          "updatedOn": "outputEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Edge",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "sourceUrn",
                "default": null,
                "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "destinationUrn",
                "doc": "Urn of the destination of this relationship edge."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "AuditStamp",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "impersonator",
                        "default": null,
                        "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "message",
                        "default": null,
                        "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                      }
                    ],
                    "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                  }
                ],
                "name": "created",
                "default": null,
                "doc": "Audit stamp containing who created this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  "com.linkedin.common.AuditStamp"
                ],
                "name": "lastModified",
                "default": null,
                "doc": "Audit stamp containing who last modified this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "map",
                    "values": "string"
                  }
                ],
                "name": "properties",
                "default": null,
                "doc": "A generic properties bag that allows us to store specific information on this graph edge."
              }
            ],
            "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
          }
        }
      ],
      "name": "outputEdges",
      "default": null,
      "doc": "Output assets produced by the data process instance during processing, with additional metadata.\nCounts as lineage.\nWill eventually deprecate the outputs field."
    }
  ],
  "doc": "Information about the outputs of a Data process"
}
```
</details>

### dataProcessInstanceProperties
The inputs and outputs of this data process
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceProperties"
  },
  "name": "DataProcessInstanceProperties",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Process name"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "processType",
        "fieldType": "KEYWORD",
        "filterNameOverride": "Process Type"
      },
      "type": [
        "null",
        {
          "type": "enum",
          "name": "DataProcessType",
          "namespace": "com.linkedin.dataprocess",
          "symbols": [
            "BATCH_SCHEDULED",
            "BATCH_AD_HOC",
            "STREAMING"
          ]
        }
      ],
      "name": "type",
      "default": null,
      "doc": "Process type"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "created",
          "fieldType": "COUNT",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "doc": "Audit stamp containing who reported the lineage and when"
    }
  ],
  "doc": "The inputs and outputs of this data process"
}
```
</details>

### dataProcessInstanceRelationships
Information about Data process relationships
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRelationships"
  },
  "name": "DataProcessInstanceRelationships",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataJob",
          "dataFlow",
          "dataset"
        ],
        "name": "InstanceOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentTemplate",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentTemplate",
      "default": null,
      "doc": "The parent entity whose run instance it is"
    },
    {
      "Relationship": {
        "entityTypes": [
          "dataProcessInstance"
        ],
        "name": "ChildOf"
      },
      "Searchable": {
        "/*": {
          "fieldName": "parentInstance",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "parentInstance",
      "default": null,
      "doc": "The parent DataProcessInstance where it belongs to.\nIf it is a Airflow Task then it should belong to an Airflow Dag run as well\nwhich will be another DataProcessInstance"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataProcessInstance"
          ],
          "name": "UpstreamOf"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "upstream",
          "fieldType": "URN",
          "numValuesFieldName": "numUpstreams",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "upstreamInstances",
      "doc": "Input DataProcessInstance which triggered this dataprocess instance"
    }
  ],
  "doc": "Information about Data process relationships"
}
```
</details>

### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted)."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```
</details>

### testResults
Information about a Test Result
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```
</details>

### dataPlatformInstance
The specific instance of the data platform that this entity belongs to
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```
</details>

### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```
</details>

### container
Link from an asset to its parent container
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
}
```
</details>

### mlTrainingRunProperties
The inputs and outputs of this training run
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlTrainingRunProperties"
  },
  "name": "MLTrainingRunProperties",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "id",
      "default": null,
      "doc": "Run Id of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outputUrls",
      "default": null,
      "doc": "List of URLs for the Outputs of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlHyperParam"
            },
            "name": "MLHyperParam",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the MLHyperParam"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the MLHyperParam was developed"
              }
            ],
            "doc": "Properties associated with an ML Hyper Param"
          }
        }
      ],
      "name": "hyperParams",
      "default": null,
      "doc": "Hyperparameters of the ML Training Run"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "Aspect": {
              "name": "mlMetric"
            },
            "name": "MLMetric",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": "string",
                "name": "name",
                "doc": "Name of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "description",
                "default": null,
                "doc": "Documentation of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "value",
                "default": null,
                "doc": "The value of the mlMetric"
              },
              {
                "type": [
                  "null",
                  "long"
                ],
                "name": "createdAt",
                "default": null,
                "doc": "Date when the mlMetric was developed"
              }
            ],
            "doc": "Properties associated with an ML Metric"
          }
        }
      ],
      "name": "trainingMetrics",
      "default": null,
      "doc": "Metrics of the ML Training Run"
    }
  ],
  "doc": "The inputs and outputs of this training run"
}
```
</details>

### dataProcessInstanceRunEvent (Timeseries)
An event representing the current status of data process run.
DataProcessRunEvent should be used for reporting the status of a dataProcess' run.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataProcessInstanceRunEvent",
    "type": "timeseries"
  },
  "name": "DataProcessInstanceRunEvent",
  "namespace": "com.linkedin.dataprocess",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "hasValuesFieldName": "hasRunEvents"
      },
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "STARTED": "The status where the Data processing run is in."
        },
        "name": "DataProcessRunStatus",
        "namespace": "com.linkedin.dataprocess",
        "symbols": [
          "STARTED",
          "COMPLETE"
        ]
      },
      "name": "status"
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "attempt",
      "default": null,
      "doc": "Return the try number that this Instance Run is in"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        {
          "type": "record",
          "name": "DataProcessInstanceRunResult",
          "namespace": "com.linkedin.dataprocess",
          "fields": [
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Run Failed",
                  "SKIPPED": " The Run Skipped",
                  "SUCCESS": " The Run Succeeded",
                  "UP_FOR_RETRY": " The Run Failed and will Retry"
                },
                "name": "RunResultType",
                "namespace": "com.linkedin.dataprocess",
                "symbols": [
                  "SUCCESS",
                  "FAILURE",
                  "SKIPPED",
                  "UP_FOR_RETRY"
                ]
              },
              "name": "type",
              "doc": " The final result, e.g. SUCCESS, FAILURE, SKIPPED, or UP_FOR_RETRY."
            },
            {
              "type": "string",
              "name": "nativeResultType",
              "doc": "It identifies the system where the native result comes from like Airflow, Azkaban, etc.."
            }
          ]
        }
      ],
      "name": "result",
      "default": null,
      "doc": "The final result of the Data Processing run."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "durationMillis",
      "default": null,
      "doc": "The duration of the run in milliseconds."
    }
  ],
  "doc": "An event representing the current status of data process run.\nDataProcessRunEvent should be used for reporting the status of a dataProcess' run."
}
```
</details>

## Relationships

### Self
These are the relationships to itself, stored in this entity's aspects
- DataProcessInstanceConsumes (via `dataProcessInstanceInput.inputEdges`)
- DataProcessInstanceProduces (via `dataProcessInstanceOutput.outputEdges`)
- ChildOf (via `dataProcessInstanceRelationships.parentInstance`)
- UpstreamOf (via `dataProcessInstanceRelationships.upstreamInstances`)
### Outgoing
These are the relationships stored in this entity's aspects
- Consumes

   - Dataset via `dataProcessInstanceInput.inputs`
   - MlModel via `dataProcessInstanceInput.inputs`
- DataProcessInstanceConsumes

   - Dataset via `dataProcessInstanceInput.inputEdges`
   - MlModel via `dataProcessInstanceInput.inputEdges`
- Produces

   - Dataset via `dataProcessInstanceOutput.outputs`
   - MlModel via `dataProcessInstanceOutput.outputs`
- DataProcessInstanceProduces

   - Dataset via `dataProcessInstanceOutput.outputEdges`
   - MlModel via `dataProcessInstanceOutput.outputEdges`
- InstanceOf

   - DataJob via `dataProcessInstanceRelationships.parentTemplate`
   - DataFlow via `dataProcessInstanceRelationships.parentTemplate`
   - Dataset via `dataProcessInstanceRelationships.parentTemplate`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
- IsPartOf

   - Container via `container.container`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
