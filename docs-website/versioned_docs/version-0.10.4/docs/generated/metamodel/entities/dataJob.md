---
sidebar_position: 3
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
        "fieldType": "TEXT_PARTIAL"
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
        "fieldType": "TEXT_PARTIAL"
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
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
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
                "hasValuesFieldName": "hasGlossaryTerms"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
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
        "fieldType": "BOOLEAN",
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
              "default": "PARTITION"
            },
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "String representation of the partition"
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
              "doc": "Time window of the partition if applicable"
            }
          ],
          "doc": "Defines how the data is partitioned"
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
              "default": "PARTITION"
            },
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "String representation of the partition"
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
              "doc": "Time window of the partition if applicable"
            }
          ],
          "doc": "Defines how the data is partitioned"
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

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
