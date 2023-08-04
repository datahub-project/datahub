---
sidebar_position: 18
title: MlModel
slug: /generated/metamodel/entities/mlmodel
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/mlModel.md
---

# MlModel

## Aspects

### mlModelKey

Key for an ML model

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelKey"
  },
  "name": "MLModelKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Standardized platform urn for the model"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "Name of the MLModel"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "TEXT_PARTIAL",
        "filterNameOverride": "Environment",
        "queryByDefault": false
      },
      "type": {
        "type": "enum",
        "symbolDocs": {
          "CORP": "Designates corporation fabrics",
          "DEV": "Designates development fabrics",
          "EI": "Designates early-integration fabrics",
          "NON_PROD": "Designates non-production fabrics",
          "PRE": "Designates pre-production fabrics",
          "PROD": "Designates production fabrics",
          "QA": "Designates quality assurance fabrics",
          "STG": "Designates staging fabrics",
          "TEST": "Designates testing fabrics",
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
          "CORP"
        ],
        "doc": "Fabric group type"
      },
      "name": "origin",
      "doc": "Fabric type where model belongs to or where it was generated"
    }
  ],
  "doc": "Key for an ML model"
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

### mlModelProperties

Properties associated with a ML Model

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelProperties"
  },
  "name": "MLModelProperties",
  "namespace": "com.linkedin.ml.metadata",
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
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Documentation of the MLModel"
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "date",
      "default": null,
      "doc": "Date when the MLModel was developed"
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "VersionTag",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": [
                "null",
                "string"
              ],
              "name": "versionTag",
              "default": null
            }
          ],
          "doc": "A resource-defined string representing the resource state for the purpose of concurrency control"
        }
      ],
      "name": "version",
      "default": null,
      "doc": "Version of the MLModel"
    },
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "type",
      "default": null,
      "doc": "Type of Algorithm or MLModel such as whether it is a Naive Bayes classifier, Convolutional Neural Network, etc"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": [
            "string",
            "int",
            "float",
            "double",
            "boolean"
          ]
        }
      ],
      "name": "hyperParameters",
      "default": null,
      "doc": "Hyper Parameters of the MLModel\n\nNOTE: these are deprecated in favor of hyperParams"
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
      "doc": "Hyperparameters of the MLModel"
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
      "doc": "Metrics of the MLModel used in training"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.ml.metadata.MLMetric"
        }
      ],
      "name": "onlineMetrics",
      "default": null,
      "doc": "Metrics of the MLModel used in production"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlFeature"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "mlFeatures",
      "default": null,
      "doc": "List of features used for MLModel training"
    },
    {
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "tags",
      "default": [],
      "doc": "Tags for the MLModel"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlModelDeployment"
          ],
          "name": "DeployedTo"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "deployments",
      "default": null,
      "doc": "Deployments for the MLModel"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "name": "TrainedBy"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "trainingJobs",
      "default": null,
      "doc": "List of jobs (if any) used to train the model"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataJob"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "UsedBy"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "downstreamJobs",
      "default": null,
      "doc": "List of jobs (if any) that use the model"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "mlModelGroup"
          ],
          "isLineage": true,
          "isUpstream": false,
          "name": "MemberOf"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "groups",
      "default": null,
      "doc": "Groups the model belongs to"
    }
  ],
  "doc": "Properties associated with a ML Model"
}
```

</details>

### intendedUse

Intended Use for the ML Model

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "intendedUse"
  },
  "name": "IntendedUse",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "primaryUses",
      "default": null,
      "doc": "Primary Use cases for the MLModel."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "enum",
            "name": "IntendedUserType",
            "namespace": "com.linkedin.ml.metadata",
            "symbols": [
              "ENTERPRISE",
              "HOBBY",
              "ENTERTAINMENT"
            ]
          }
        }
      ],
      "name": "primaryUsers",
      "default": null,
      "doc": "Primary Intended Users - For example, was the MLModel developed for entertainment purposes, for hobbyists, or enterprise solutions?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "outOfScopeUses",
      "default": null,
      "doc": "Highlight technology that the MLModel might easily be confused with, or related contexts that users could try to apply the MLModel to."
    }
  ],
  "doc": "Intended Use for the ML Model"
}
```

</details>

### mlModelFactorPrompts

Prompts which affect the performance of the MLModel

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelFactorPrompts"
  },
  "name": "MLModelFactorPrompts",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "MLModelFactors",
            "namespace": "com.linkedin.ml.metadata",
            "fields": [
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "groups",
                "default": null,
                "doc": "Groups refers to distinct categories with similar characteristics that are present in the evaluation data instances.\nFor human-centric machine learning MLModels, groups are people who share one or multiple characteristics."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "instrumentation",
                "default": null,
                "doc": "The performance of a MLModel can vary depending on what instruments were used to capture the input to the MLModel.\nFor example, a face detection model may perform differently depending on the camera's hardware and software,\nincluding lens, image stabilization, high dynamic range techniques, and background blurring for portrait mode."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "array",
                    "items": "string"
                  }
                ],
                "name": "environment",
                "default": null,
                "doc": "A further factor affecting MLModel performance is the environment in which it is deployed."
              }
            ],
            "doc": "Factors affecting the performance of the MLModel."
          }
        }
      ],
      "name": "relevantFactors",
      "default": null,
      "doc": "What are foreseeable salient factors for which MLModel performance may vary, and how were these determined?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.ml.metadata.MLModelFactors"
        }
      ],
      "name": "evaluationFactors",
      "default": null,
      "doc": "Which factors are being reported, and why were these chosen?"
    }
  ],
  "doc": "Prompts which affect the performance of the MLModel"
}
```

</details>

### mlModelMetrics

Metrics to be featured for the MLModel.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelMetrics"
  },
  "name": "Metrics",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "performanceMeasures",
      "default": null,
      "doc": "Measures of MLModel performance"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "decisionThreshold",
      "default": null,
      "doc": "Decision Thresholds used (if any)?"
    }
  ],
  "doc": "Metrics to be featured for the MLModel."
}
```

</details>

### mlModelEvaluationData

All referenced datasets would ideally point to any set of documents that provide visibility into the source and composition of the dataset.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelEvaluationData"
  },
  "name": "EvaluationData",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BaseData",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "What dataset were used in the MLModel?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "motivation",
              "default": null,
              "doc": "Why was this dataset chosen?"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "preProcessing",
              "default": null,
              "doc": "How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"
            }
          ],
          "doc": "BaseData record"
        }
      },
      "name": "evaluationData",
      "doc": "Details on the dataset(s) used for the quantitative analyses in the MLModel"
    }
  ],
  "doc": "All referenced datasets would ideally point to any set of documents that provide visibility into the source and composition of the dataset."
}
```

</details>

### mlModelTrainingData

Ideally, the MLModel card would contain as much information about the training data as the evaluation data. However, there might be cases where it is not feasible to provide this level of detailed information about the training data. For example, the data may be proprietary, or require a non-disclosure agreement. In these cases, we advocate for basic details about the distributions over groups in the data, as well as any other details that could inform stakeholders on the kinds of biases the model may have encoded.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelTrainingData"
  },
  "name": "TrainingData",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BaseData",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.DatasetUrn"
              },
              "type": "string",
              "name": "dataset",
              "doc": "What dataset were used in the MLModel?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "motivation",
              "default": null,
              "doc": "Why was this dataset chosen?"
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "preProcessing",
              "default": null,
              "doc": "How was the data preprocessed (e.g., tokenization of sentences, cropping of images, any filtering such as dropping images without faces)?"
            }
          ],
          "doc": "BaseData record"
        }
      },
      "name": "trainingData",
      "doc": "Details on the dataset(s) used for training the MLModel"
    }
  ],
  "doc": "Ideally, the MLModel card would contain as much information about the training data as the evaluation data. However, there might be cases where it is not feasible to provide this level of detailed information about the training data. For example, the data may be proprietary, or require a non-disclosure agreement. In these cases, we advocate for basic details about the distributions over groups in the data, as well as any other details that could inform stakeholders on the kinds of biases the model may have encoded."
}
```

</details>

### mlModelQuantitativeAnalyses

Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative analyses should provide the results of evaluating the MLModel according to the chosen metrics, providing confidence interval values when possible.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelQuantitativeAnalyses"
  },
  "name": "QuantitativeAnalyses",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "unitaryResults",
      "default": null,
      "doc": "Link to a dashboard with results showing how the MLModel performed with respect to each factor"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "intersectionalResults",
      "default": null,
      "doc": "Link to a dashboard with results showing how the MLModel performed with respect to the intersection of evaluated factors?"
    }
  ],
  "doc": "Quantitative analyses should be disaggregated, that is, broken down by the chosen factors. Quantitative analyses should provide the results of evaluating the MLModel according to the chosen metrics, providing confidence interval values when possible."
}
```

</details>

### mlModelEthicalConsiderations

This section is intended to demonstrate the ethical considerations that went into MLModel development, surfacing ethical challenges and solutions to stakeholders.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelEthicalConsiderations"
  },
  "name": "EthicalConsiderations",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "data",
      "default": null,
      "doc": "Does the MLModel use any sensitive data (e.g., protected classes)?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "humanLife",
      "default": null,
      "doc": " Is the MLModel intended to inform decisions about matters central to human life or flourishing - e.g., health or safety? Or could it be used in such a way?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "mitigations",
      "default": null,
      "doc": "What risk mitigation strategies were used during MLModel development?"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "risksAndHarms",
      "default": null,
      "doc": "What risks may be present in MLModel usage? Try to identify the potential recipients, likelihood, and magnitude of harms. If these cannot be determined, note that they were considered but remain unknown."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "useCases",
      "default": null,
      "doc": "Are there any known MLModel use cases that are especially fraught? This may connect directly to the intended use section"
    }
  ],
  "doc": "This section is intended to demonstrate the ethical considerations that went into MLModel development, surfacing ethical challenges and solutions to stakeholders."
}
```

</details>

### mlModelCaveatsAndRecommendations

This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "mlModelCaveatsAndRecommendations"
  },
  "name": "CaveatsAndRecommendations",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "CaveatDetails",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "type": [
                "null",
                "boolean"
              ],
              "name": "needsFurtherTesting",
              "default": null,
              "doc": "Did the results suggest any further testing?"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "caveatDescription",
              "default": null,
              "doc": "Caveat Description\nFor ex: Given gender classes are binary (male/not male), which we include as male/female. Further work needed to evaluate across a spectrum of genders."
            },
            {
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ],
              "name": "groupsNotRepresented",
              "default": null,
              "doc": "Relevant groups that were not represented in the evaluation dataset?"
            }
          ],
          "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"
        }
      ],
      "name": "caveats",
      "default": null,
      "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset?"
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "recommendations",
      "default": null,
      "doc": "Recommendations on where this MLModel should be used."
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "name": "idealDatasetCharacteristics",
      "default": null,
      "doc": "Ideal characteristics of an evaluation dataset for this MLModel"
    }
  ],
  "doc": "This section should list additional concerns that were not covered in the previous sections. For example, did the results suggest any further testing? Were there any relevant groups that were not represented in the evaluation dataset? Are there additional recommendations for model use?"
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

### sourceCode

Source Code

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "sourceCode"
  },
  "name": "SourceCode",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "SourceCodeUrl",
          "namespace": "com.linkedin.ml.metadata",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "SourceCodeUrlType",
                "namespace": "com.linkedin.ml.metadata",
                "symbols": [
                  "ML_MODEL_SOURCE_CODE",
                  "TRAINING_PIPELINE_SOURCE_CODE",
                  "EVALUATION_PIPELINE_SOURCE_CODE"
                ]
              },
              "name": "type",
              "doc": "Source Code Url Types"
            },
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "sourceCodeUrl",
              "doc": "Source Code Url"
            }
          ],
          "doc": "Source Code Url Entity"
        }
      },
      "name": "sourceCode",
      "doc": "Source Code along with types"
    }
  ],
  "doc": "Source Code"
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

### cost

None

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "cost"
  },
  "name": "Cost",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ORG_COST_TYPE": "Org Cost Type to which the Cost of this entity should be attributed to"
        },
        "name": "CostType",
        "namespace": "com.linkedin.common",
        "symbols": [
          "ORG_COST_TYPE"
        ],
        "doc": "Type of Cost Code"
      },
      "name": "costType"
    },
    {
      "type": {
        "type": "record",
        "name": "CostCost",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": [
              "null",
              "double"
            ],
            "name": "costId",
            "default": null
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "costCode",
            "default": null
          },
          {
            "type": {
              "type": "enum",
              "name": "CostCostDiscriminator",
              "namespace": "com.linkedin.common",
              "symbols": [
                "costId",
                "costCode"
              ]
            },
            "name": "fieldDiscriminator",
            "doc": "Contains the name of the field that has its value set."
          }
        ]
      },
      "name": "cost"
    }
  ]
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

### editableMlModelProperties

Properties associated with a ML Model editable from the UI

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableMlModelProperties"
  },
  "name": "EditableMLModelProperties",
  "namespace": "com.linkedin.ml.metadata",
  "fields": [
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
      "doc": "Documentation of the ml model"
    }
  ],
  "doc": "Properties associated with a ML Model editable from the UI"
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

## Relationships

### Outgoing

These are the relationships stored in this entity's aspects

- OwnedBy

  - Corpuser via `ownership.owners.owner`
  - CorpGroup via `ownership.owners.owner`

- ownershipType

  - OwnershipType via `ownership.owners.typeUrn`

- Consumes

  - MlFeature via `mlModelProperties.mlFeatures`

- DeployedTo

  - MlModelDeployment via `mlModelProperties.deployments`

- TrainedBy

  - DataJob via `mlModelProperties.trainingJobs`

- UsedBy

  - DataJob via `mlModelProperties.downstreamJobs`

- MemberOf

  - MlModelGroup via `mlModelProperties.groups`

- TaggedWith

  - Tag via `globalTags.tags`

- TermedWith

  - GlossaryTerm via `glossaryTerms.terms.urn`

- AssociatedWith

  - Domain via `domains.domains`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
