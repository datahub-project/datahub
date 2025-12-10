---
sidebar_position: 2
title: DataJob
slug: /generated/metamodel/entities/datajob
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataJob.md
---
# DataJob
## Aspects

### dataJobKey
Key for a Data Job
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobKey"
  },
  "name": "DataJobKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "dataFlow"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "fieldName": "dataFlow",
        "fieldType": "URN_PARTIAL",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "flow",
      "doc": "Standardized data processing flow urn representing the flow for the job"
    },
    {
      "Searchable": {
        "enableAutocomplete": true,
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "jobId",
      "doc": "Unique Identifier of the data job"
    }
  ],
  "doc": "Key for a Data Job"
}
```
</details>

### dataJobInfo
Information about a Data processing job
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobInfo"
  },
  "name": "DataJobInfo",
  "namespace": "com.linkedin.datajob",
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
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM"
      },
      "type": "string",
      "name": "name",
      "doc": "Job name"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Job description"
    },
    {
      "type": [
        {
          "type": "enum",
          "symbolDocs": {
            "COMMAND": "The command job type is one of the basic built-in types. It runs multiple UNIX commands using java processbuilder.\nUpon execution, Azkaban spawns off a process to run the command.",
            "GLUE": "Glue type is for running AWS Glue job transforms.",
            "HADOOP_JAVA": "Runs a java program with ability to access Hadoop cluster.\nhttps://azkaban.readthedocs.io/en/latest/jobTypes.html#java-job-type",
            "HADOOP_SHELL": "In large part, this is the same Command type. The difference is its ability to talk to a Hadoop cluster\nsecurely, via Hadoop tokens.",
            "HIVE": "Hive type is for running Hive jobs.",
            "PIG": "Pig type is for running Pig jobs.",
            "SQL": "SQL is for running Presto, mysql queries etc"
          },
          "name": "AzkabanJobType",
          "namespace": "com.linkedin.datajob.azkaban",
          "symbols": [
            "COMMAND",
            "HADOOP_JAVA",
            "HADOOP_SHELL",
            "HIVE",
            "PIG",
            "SQL",
            "GLUE"
          ],
          "doc": "The various types of support azkaban jobs"
        },
        "string"
      ],
      "name": "type",
      "doc": "Datajob type\n*NOTE**: AzkabanJobType is deprecated. Please use strings instead."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.DataFlowUrn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "flowUrn",
      "default": null,
      "doc": "DataFlow urn that this job is part of"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "createdAt",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the event occur"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "Optional: The actor urn involved in the event."
            }
          ],
          "doc": "A standard event timestamp"
        }
      ],
      "name": "created",
      "default": null,
      "doc": "A timestamp documenting when the asset was created in the source Data Platform (not on DataHub)"
    },
    {
      "Searchable": {
        "/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME"
        }
      },
      "type": [
        "null",
        "com.linkedin.common.TimeStamp"
      ],
      "name": "lastModified",
      "default": null,
      "doc": "A timestamp documenting when the asset was last modified in the source Data Platform (not on DataHub)"
    },
    {
      "deprecated": "Use Data Process Instance model, instead",
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "COMPLETED": "Jobs with successful completion.",
            "FAILED": "Jobs that have failed.",
            "IN_PROGRESS": "Jobs currently running.",
            "SKIPPED": "Jobs that have been skipped.",
            "STARTING": "Jobs being initialized.",
            "STOPPED": "Jobs that have stopped.",
            "STOPPING": "Jobs being stopped.",
            "UNKNOWN": "Jobs with unknown status (either unmappable or unavailable)"
          },
          "name": "JobStatus",
          "namespace": "com.linkedin.datajob",
          "symbols": [
            "STARTING",
            "IN_PROGRESS",
            "STOPPING",
            "STOPPED",
            "COMPLETED",
            "FAILED",
            "UNKNOWN",
            "SKIPPED"
          ],
          "doc": "Job statuses"
        }
      ],
      "name": "status",
      "default": null,
      "doc": "Status of the job - Deprecated for Data Process Instance model."
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Environment",
        "queryByDefault": false
      },
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "CORP": "Designates corporation fabrics",
            "DEV": "Designates development fabrics",
            "EI": "Designates early-integration fabrics",
            "NON_PROD": "Designates non-production fabrics",
            "PRD": "Alternative Prod spelling",
            "PRE": "Designates pre-production fabrics",
            "PROD": "Designates production fabrics",
            "QA": "Designates quality assurance fabrics",
            "RVW": "Designates review fabrics",
            "SANDBOX": "Designates sandbox fabrics",
            "SBX": "Alternative spelling for sandbox",
            "SIT": "System Integration Testing",
            "STG": "Designates staging fabrics",
            "TEST": "Designates testing fabrics",
            "TST": "Alternative Test spelling",
            "UAT": "Designates user acceptance testing fabrics"
          },
          "name": "FabricType",
          "namespace": "com.linkedin.common",
          "symbols": [
            "DEV",
            "TEST",
            "QA",
            "UAT",
            "EI",
            "PRE",
            "STG",
            "NON_PROD",
            "PROD",
            "CORP",
            "RVW",
            "PRD",
            "TST",
            "SIT",
            "SBX",
            "SANDBOX"
          ],
          "doc": "Fabric group type"
        }
      ],
      "name": "env",
      "default": null,
      "doc": "Environment for this job"
    }
  ],
  "doc": "Information about a Data processing job"
}
```
</details>

### dataJobInputOutput
Information about the inputs and outputs of a Data processing job
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataJobInputOutput"
  },
  "name": "DataJobInputOutput",
  "namespace": "com.linkedin.datajob",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "inputs",
          "fieldType": "URN",
          "numValuesFieldName": "numInputDatasets",
          "queryByDefault": false
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "inputDatasets",
      "doc": "Input datasets consumed by the data job during processing\nDeprecated! Use inputDatasetEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputDatasetEdges/*/created/actor",
          "createdOn": "inputDatasetEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes",
          "properties": "inputDatasetEdges/*/properties",
          "updatedActor": "inputDatasetEdges/*/lastModified/actor",
          "updatedOn": "inputDatasetEdges/*/lastModified/time"
        }
      },
      "Searchable": {
        "/*/destinationUrn": {
          "fieldName": "inputDatasetEdges",
          "fieldType": "URN",
          "numValuesFieldName": "numInputDatasets",
          "queryByDefault": false
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
      "name": "inputDatasetEdges",
      "default": null,
      "doc": "Input datasets consumed by the data job during processing"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "outputs",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputDatasets",
          "queryByDefault": false
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "outputDatasets",
      "doc": "Output datasets produced by the data job during processing\nDeprecated! Use outputDatasetEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "outputDatasetEdges/*/created/actor",
          "createdOn": "outputDatasetEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "Produces",
          "properties": "outputDatasetEdges/*/properties",
          "updatedActor": "outputDatasetEdges/*/lastModified/actor",
          "updatedOn": "outputDatasetEdges/*/lastModified/time"
        }
      },
      "Searchable": {
        "/*/destinationUrn": {
          "fieldName": "outputDatasetEdges",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputDatasets",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.common.Edge"
        }
      ],
      "name": "outputDatasetEdges",
      "default": null,
      "doc": "Output datasets produced by the data job during processing"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "name": "DownstreamOf"
        }
      },
      "deprecated": true,
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "inputDatajobs",
      "default": null,
      "doc": "Input datajobs that this data job depends on\nDeprecated! Use inputDatajobEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "inputDatajobEdges/*/created/actor",
          "createdOn": "inputDatajobEdges/*/created/time",
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "name": "DownstreamOf",
          "properties": "inputDatajobEdges/*/properties",
          "updatedActor": "inputDatajobEdges/*/lastModified/actor",
          "updatedOn": "inputDatajobEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.common.Edge"
        }
      ],
      "name": "inputDatajobEdges",
      "default": null,
      "doc": "Input datajobs that this data job depends on"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "schemaField"
          ],
          "name": "Consumes"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "inputFields",
          "fieldType": "URN",
          "numValuesFieldName": "numInputFields",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "inputDatasetFields",
      "default": null,
      "doc": "Fields of the input datasets used by this job"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "schemaField"
          ],
          "name": "Produces"
        }
      },
      "Searchable": {
        "/*": {
          "fieldName": "outputFields",
          "fieldType": "URN",
          "numValuesFieldName": "numOutputFields",
          "queryByDefault": false
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outputDatasetFields",
      "default": null,
      "doc": "Fields of the output datasets this job writes to"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "FineGrainedLineage",
            "namespace": "com.linkedin.dataset",
            "fields": [
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "DATASET": " Indicates that this lineage is originating from upstream dataset(s)",
                    "FIELD_SET": " Indicates that this lineage is originating from upstream field(s)",
                    "NONE": " Indicates that there is no upstream lineage i.e. the downstream field is not a derived field"
                  },
                  "name": "FineGrainedLineageUpstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD_SET",
                    "DATASET",
                    "NONE"
                  ],
                  "doc": "The type of upstream entity in a fine-grained lineage"
                },
                "name": "upstreamType",
                "doc": "The type of upstream entity"
              },
              {
                "Searchable": {
                  "/*": {
                    "fieldName": "fineGrainedUpstreams",
                    "fieldType": "URN",
                    "hasValuesFieldName": "hasFineGrainedUpstreams",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "upstreams",
                "default": null,
                "doc": "Upstream entities in the lineage"
              },
              {
                "type": {
                  "type": "enum",
                  "symbolDocs": {
                    "FIELD": " Indicates that the lineage is for a single, specific, downstream field",
                    "FIELD_SET": " Indicates that the lineage is for a set of downstream fields"
                  },
                  "name": "FineGrainedLineageDownstreamType",
                  "namespace": "com.linkedin.dataset",
                  "symbols": [
                    "FIELD",
                    "FIELD_SET"
                  ],
                  "doc": "The type of downstream field(s) in a fine-grained lineage"
                },
                "name": "downstreamType",
                "doc": "The type of downstream field(s)"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "downstreams",
                "default": null,
                "doc": "Downstream fields in the lineage"
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "transformOperation",
                "default": null,
                "doc": "The transform operation applied to the upstream entities to produce the downstream field(s)"
              },
              {
                "type": "float",
                "name": "confidenceScore",
                "default": 1.0,
                "doc": "The confidence in this lineage between 0 (low confidence) and 1 (high confidence)"
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "query",
                "default": null,
                "doc": "The query that was used to generate this lineage. \nPresent only if the lineage was generated from a detected query."
              }
            ],
            "doc": "A fine-grained lineage from upstream fields/datasets to downstream field(s)"
          }
        }
      ],
      "name": "fineGrainedLineages",
      "default": null,
      "doc": "Fine-grained column-level lineages\nNot currently supported in the UI\nUse UpstreamLineage aspect for datasets to express Column Level Lineage for the UI"
    }
  ],
  "doc": "Information about the inputs and outputs of a Data processing job"
}
```
</details>

### editableDataJobProperties
Stores editable changes made to properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableDataJobProperties"
  },
  "name": "EditableDataJobProperties",
  "namespace": "com.linkedin.datajob",
  "fields": [
    {
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
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Edited documentation of the data job "
    }
  ],
  "doc": "Stores editable changes made to properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
}
```
</details>

### ownership
Ownership information of an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/*": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
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
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
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

### globalTags
Tag aspect used for applying tags to an entity
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tag",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```
</details>

### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```
</details>

### glossaryTerms
Related business terms information
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
    },
    {
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
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```
</details>

### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
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
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
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

### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```
</details>

### domains
Links from an Asset to its Domains
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains"
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```
</details>

### applications
Links from an Asset to its Applications
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```
</details>

### deprecation
Deprecation status of an entity
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
}
```
</details>

### versionInfo
Information about a Data processing job
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionInfo"
  },
  "name": "VersionInfo",
  "namespace": "com.linkedin.datajob",
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
      "type": "string",
      "name": "version",
      "doc": "The version which can indentify a job version like a commit hash or md5 hash"
    },
    {
      "type": "string",
      "name": "versionType",
      "doc": "The type of the version like git hash or md5 hash"
    }
  ],
  "doc": "Information about a Data processing job"
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

### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
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
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```
</details>

### forms
Forms that are assigned to this entity to be filled out
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
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
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
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

### incidentsSummary
Summary related incidents on an entity.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
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

### dataTransformLogic
Information about a Query against one or more data assets (e.g. Tables or Views).
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataTransformLogic"
  },
  "name": "DataTransformLogic",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DataTransform",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "QueryStatement",
                  "namespace": "com.linkedin.query",
                  "fields": [
                    {
                      "type": "string",
                      "name": "value",
                      "doc": "The query text"
                    },
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "SQL": "A SQL Query",
                          "UNKNOWN": "Unknown query language"
                        },
                        "name": "QueryLanguage",
                        "namespace": "com.linkedin.query",
                        "symbols": [
                          "SQL",
                          "UNKNOWN"
                        ]
                      },
                      "name": "language",
                      "default": "SQL",
                      "doc": "The language of the Query, e.g. SQL."
                    }
                  ],
                  "doc": "A query statement against one or more data assets."
                }
              ],
              "name": "queryStatement",
              "default": null,
              "doc": "The data transform may be defined by a query statement"
            }
          ],
          "doc": "Information about a transformation. It may be a query,"
        }
      },
      "name": "transforms",
      "doc": "List of transformations applied"
    }
  ],
  "doc": "Information about a Query against one or more data assets (e.g. Tables or Views)."
}
```
</details>

### datahubIngestionRunSummary (Timeseries)
Summary of a datahub ingestion run for a given platform.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datahubIngestionRunSummary",
    "type": "timeseries"
  },
  "name": "DatahubIngestionRunSummary",
  "namespace": "com.linkedin.datajob.datahub",
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
      "TimeseriesField": {},
      "type": "string",
      "name": "pipelineName",
      "doc": "The name of the pipeline that ran ingestion, a stable unique user provided identifier.\n e.g. my_snowflake1-to-datahub."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "platformInstanceId",
      "doc": "The id of the instance against which the ingestion pipeline ran.\ne.g.: Bigquery project ids, MySQL hostnames etc."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "runId",
      "doc": "The runId for this pipeline instance."
    },
    {
      "TimeseriesField": {},
      "type": {
        "type": "enum",
        "symbolDocs": {
          "COMPLETED": "Jobs with successful completion.",
          "FAILED": "Jobs that have failed.",
          "IN_PROGRESS": "Jobs currently running.",
          "SKIPPED": "Jobs that have been skipped.",
          "STARTING": "Jobs being initialized.",
          "STOPPED": "Jobs that have stopped.",
          "STOPPING": "Jobs being stopped.",
          "UNKNOWN": "Jobs with unknown status (either unmappable or unavailable)"
        },
        "name": "JobStatus",
        "namespace": "com.linkedin.datajob",
        "symbols": [
          "STARTING",
          "IN_PROGRESS",
          "STOPPING",
          "STOPPED",
          "COMPLETED",
          "FAILED",
          "UNKNOWN",
          "SKIPPED"
        ],
        "doc": "Job statuses"
      },
      "name": "runStatus",
      "doc": "Run Status - Succeeded/Skipped/Failed etc."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWorkUnitsCommitted",
      "default": null,
      "doc": "The number of workunits written to sink."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWorkUnitsCreated",
      "default": null,
      "doc": "The number of workunits that are produced."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEvents",
      "default": null,
      "doc": "The number of events produced (MCE + MCP)."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEntities",
      "default": null,
      "doc": "The total number of entities produced (unique entity urns)."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numAspects",
      "default": null,
      "doc": "The total number of aspects produced across all entities."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numSourceAPICalls",
      "default": null,
      "doc": "Total number of source API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalLatencySourceAPICalls",
      "default": null,
      "doc": "Total latency across all source API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numSinkAPICalls",
      "default": null,
      "doc": "Total number of sink API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalLatencySinkAPICalls",
      "default": null,
      "doc": "Total latency across all sink API calls."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numWarnings",
      "default": null,
      "doc": "Number of warnings generated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numErrors",
      "default": null,
      "doc": "Number of errors generated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "numEntitiesSkipped",
      "default": null,
      "doc": "Number of entities skipped."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "config",
      "default": null,
      "doc": "The non-sensitive key-value pairs of the yaml config used as json string."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "custom_summary",
      "default": null,
      "doc": "Custom value."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "string"
      ],
      "name": "softwareVersion",
      "default": null,
      "doc": "The software version of this ingestion."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "systemHostName",
      "default": null,
      "doc": "The hostname the ingestion pipeline ran on."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "string"
      ],
      "name": "operatingSystemName",
      "default": null,
      "doc": "The os the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "int"
      ],
      "name": "numProcessors",
      "default": null,
      "doc": "The number of processors on the host the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "totalMemory",
      "default": null,
      "doc": "The total amount of memory on the host the ingestion pipeline ran on."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "availableMemory",
      "default": null,
      "doc": "The available memory on the host the ingestion pipeline ran on."
    }
  ],
  "doc": "Summary of a datahub ingestion run for a given platform."
}
```
</details>

### datahubIngestionCheckpoint (Timeseries)
Checkpoint of a datahub ingestion run for a given job.
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "datahubIngestionCheckpoint",
    "type": "timeseries"
  },
  "name": "DatahubIngestionCheckpoint",
  "namespace": "com.linkedin.datajob.datahub",
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
      "TimeseriesField": {},
      "type": "string",
      "name": "pipelineName",
      "doc": "The name of the pipeline that ran ingestion, a stable unique user provided identifier.\n e.g. my_snowflake1-to-datahub."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "platformInstanceId",
      "doc": "The id of the instance against which the ingestion pipeline ran.\ne.g.: Bigquery project ids, MySQL hostnames etc."
    },
    {
      "type": "string",
      "name": "config",
      "doc": "Json-encoded string representation of the non-secret members of the config ."
    },
    {
      "type": {
        "type": "record",
        "name": "IngestionCheckpointState",
        "namespace": "com.linkedin.datajob.datahub",
        "fields": [
          {
            "type": "string",
            "name": "formatVersion",
            "doc": "The version of the state format."
          },
          {
            "type": "string",
            "name": "serde",
            "doc": "The serialization/deserialization protocol."
          },
          {
            "type": [
              "null",
              "bytes"
            ],
            "name": "payload",
            "default": null,
            "doc": "Opaque blob of the state representation."
          }
        ],
        "doc": "The checkpoint state object of a datahub ingestion run for a given job."
      },
      "name": "state",
      "doc": "Opaque blob of the state representation."
    },
    {
      "TimeseriesField": {},
      "type": "string",
      "name": "runId",
      "doc": "The run identifier of this job."
    }
  ],
  "doc": "Checkpoint of a datahub ingestion run for a given job."
}
```
</details>

## Relationships

### Self
These are the relationships to itself, stored in this entity's aspects
- DownstreamOf (via `dataJobInputOutput.inputDatajobs`)
- DownstreamOf (via `dataJobInputOutput.inputDatajobEdges`)
### Outgoing
These are the relationships stored in this entity's aspects
- IsPartOf

   - DataFlow via `dataJobKey.flow`
   - Container via `container.container`
- Consumes

   - Dataset via `dataJobInputOutput.inputDatasets`
   - Dataset via `dataJobInputOutput.inputDatasetEdges`
   - SchemaField via `dataJobInputOutput.inputDatasetFields`
- Produces

   - Dataset via `dataJobInputOutput.outputDatasets`
   - Dataset via `dataJobInputOutput.outputDatasetEdges`
   - SchemaField via `dataJobInputOutput.outputDatasetFields`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
