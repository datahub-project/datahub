# DataHub GMS GraphQL Service

> **Disclaimer**: DataHub's standalone GraphQL Service is now deprecated. The GraphQL API is now served from the [Metadata Service](../metadata-service/README.md) directly.
> To explore the GraphQL Query & Mutation types, visit `<your-datahub-url>/api/graphiql`. 

Datahub GMS GraphQL Service wraps the Generalized Metadata Store (GMS) Rest.li calls around a GraphQL API.

## Pre-requisites
* You need to have [JDK8](https://www.oracle.com/java/technologies/jdk8-downloads.html) 
installed on your machine to be able to build `Datahub GMS GraphQL Service`.

## Build
To build `Datahub GMS GraphQL Service`

`
./gradlew :datahub-gms-graphql-service:build
`

## Dependencies

Before starting `Datahub GMS GraphQL Service`, you need to make sure that [DataHub GMS](../metadata-service/README.md) is up and running.

## Start via Docker image
Quickest way to try out `Datahub GMS Graphql Service` is running the [Docker image](../docker/datahub-gms-graphql-service).

## Start via command line

If you do modify things and want to try it out quickly without building the Docker image, you can also run
the application directly from command line after a successful [build](#build):
```
./gradlew :datahub-gms-graphql-service:bootRun
```

## API Calls

Inorder to Start using the graphql API we would recommend you download [GraphiQL](https://www.electronjs.org/apps/graphiql)

`Endpoint`: http://localhost:8091/graphql

## Sample API Calls

### Query Dataset

Request: 
```
{
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)") {
    urn
    platform
    name
    origin
    description
    platformNativeType
    uri
    tags
    ownership {
      owners {
        owner {
          username
          urn
          info {
            displayName
            email
            fullName
            manager {
              urn
            }
          }
          editableInfo {
            aboutMe
            skills
          }
        }
        type
        source {
          url
        }
      }
      lastModified {
        actor
      }
    }
    created {
      actor
    }
    lastModified {
      actor
    }
  }
}
```

Sample Response:

```
{
  "data": {
    "dataset": {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
      "platform": "urn:li:dataPlatform:foo",
      "name": "bar",
      "origin": "PROD",
      "description": "Sample Dataset",
      "platformNativeType": null,
      "uri": null,
      "tags": [
        "Datahub",
        "Sample"
      ],
      "ownership": {
        "owners": [
          {
            "owner": {
              "username": "fbar",
              "urn": "urn:li:corpuser:fbar",
              "info": {
                "displayName": "Foo Bar",
                "email": "fbar@linkedin.com",
                "fullName": "Foo Bar",
                "manager": {
                  "urn": "urn:li:corpuser:datahub"
                }
              },
              "editableInfo": {
                "aboutMe": "About Me",
                "skills": [
                  "Java",
                  "SQL"
                ]
              }
            },
            "type": "DATAOWNER",
            "source": null
          }
        ],
        "lastModified": {
          "actor": "urn:li:corpuser:fbar"
        }
      },
      "created": {
        "actor": "urn:li:corpuser:fbar"
      },
      "lastModified": {
        "actor": "urn:li:corpuser:fbar"
      }
    }
  }
}
```

### Query MLModel

Sample Request:

```
{
  mlModel(urn: "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)") {
    urn
    type
    name
    origin
    description
    tags
    ownership {
      owners {
        owner {
          urn
          username
          editableInfo {
            pictureLink
          }
          info {
            firstName
          }
        }
        type
        source {
          type
          url
        }
      }
    }
    properties {
      description
      date
      version
      type
      hyperParameters {
        key
        value {
          ...on StringBox {
            stringValue
          }
          ... on IntBox {
            intValue
          }
          ... on FloatBox {
            floatValue
          }
          ... on BooleanBox {
            booleanValue
          }
        }
      }
      mlFeatures
      tags
    }
    status {
      removed
    }
    institutionalMemory {
      elements {
        url
        description
        created {
          actor
        }
      }
    }
    intendedUse {
      primaryUses
      primaryUsers
      outOfScopeUses
    }
    factorPrompts {
      relevantFactors {
        groups
        instrumentation
        environment
      }
      evaluationFactors {
        groups
        instrumentation
        environment
      }
    }
    metrics {
      decisionThreshold
      performanceMeasures
    }
    trainingData {
      dataset
      motivation
      preProcessing
    }
    evaluationData {
      dataset
      motivation
      preProcessing
    }
    quantitativeAnalyses {
      unitaryResults {
        ...on StringBox {
          stringValue
        }
      }
      intersectionalResults {
        ...on StringBox {
          stringValue
        }
      }
    }
    ethicalConsiderations {
      useCases
      humanLife
      mitigations
      risksAndHarms
      useCases
      data
    }
    caveatsAndRecommendations {
      caveats {
        caveatDescription
        needsFurtherTesting
        groupsNotRepresented
      }
      recommendations 
      idealDatasetCharacteristics
    }
    cost {
      costType
      costValue {
        costId
        costCode
      }
    }
  }
}
```

Sample Response:

```
{
  "data": {
    "mlModel": {
      "urn": "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)",
      "type": "MLMODEL",
      "name": "scienceModel",
      "origin": "PROD",
      "description": "A sample model for predicting some outcome.",
      "tags": [
        "Sample"
      ],
      "ownership": {
        "owners": [
          {
            "owner": {
              "urn": "urn:li:corpuser:jdoe",
              "username": "jdoe",
              "editableInfo": null,
              "info": {
                "firstName": null
              }
            },
            "type": "DATAOWNER",
            "source": null
          },
          {
            "owner": {
              "urn": "urn:li:corpuser:datahub",
              "username": "datahub",
              "editableInfo": {
                "pictureLink": "https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web-react/src/images/default_avatar.png"
              },
              "info": {
                "firstName": null
              }
            },
            "type": "DATAOWNER",
            "source": null
          }
        ]
      },
      "properties": {
        "description": "A sample model for predicting some outcome.",
        "date": null,
        "version": null,
        "type": "Naive Bayes classifier",
        "hyperParameters": null,
        "mlFeatures": null,
        "tags": [
          "Sample"
        ]
      },
      "status": {
        "removed": false
      },
      "institutionalMemory": {
        "elements": [
          {
            "url": "https://www.linkedin.com",
            "description": "Sample doc",
            "created": {
              "actor": "urn:li:corpuser:jdoe"
            }
          }
        ]
      },
      "intendedUse": {
        "primaryUses": [
          "Sample Model",
          "Primary Use"
        ],
        "primaryUsers": [
          "ENTERPRISE"
        ],
        "outOfScopeUses": [
          "Production Deployment"
        ]
      },
      "factorPrompts": null,
      "metrics": {
        "decisionThreshold": [
          "decisionThreshold"
        ],
        "performanceMeasures": [
          "performanceMeasures"
        ]
      },
      "trainingData": [
        {
          "dataset": "urn:li:dataset:(urn:li:dataPlatform:hive,pageViewsHive,PROD)",
          "motivation": "For science!",
          "preProcessing": [
            "Aggregation"
          ]
        }
      ],
      "evaluationData": [
        {
          "dataset": "urn:li:dataset:(urn:li:dataPlatform:hive,pageViewsHive,PROD)",
          "motivation": null,
          "preProcessing": null
        }
      ],
      "quantitativeAnalyses": null,
      "ethicalConsiderations": {
        "useCases": [
          "useCases"
        ],
        "humanLife": [
          "humanLife"
        ],
        "mitigations": [
          "mitigations"
        ],
        "risksAndHarms": [
          "risksAndHarms"
        ],
        "data": [
          "data"
        ]
      },
      "caveatsAndRecommendations": {
        "caveats": null,
        "recommendations": "recommendations",
        "idealDatasetCharacteristics": [
          "idealDatasetCharacteristics"
        ]
      },
      "cost": {
        "costType": "ORG_COST_TYPE",
        "costValue": {
          "costId": null,
          "costCode": "costCode"
        }
      }
    }
  }
}
```

### Query DataFlow

Request:

```
{
  dataFlow(urn: "urn:li:dataFlow:(airflow,flow1,foo)") {
    urn
    type
    orchestrator
    flowId
    info {
      name
      description
      project
    }
    ownership {
      owners {
        owner {
          username
          urn
          info {
            displayName
            email
            fullName
            manager {
              urn
            }
          }
          editableInfo {
            aboutMe
            skills
          }
        }
        type
        source {
          url
        }
      }
      lastModified {
        actor
      }
    }
	}
}
```

Sample response:

```
{
  "data": {
    "dataFlow": {
      "urn": "urn:li:dataFlow:(airflow,flow1,foo)",
      "type": "DATA_FLOW",
      "orchestrator": "airflow",
      "flowId": "flow1",
      "info": {
        "name": "flow1",
        "description": "My own workflow",
        "project": "X"
      },
      "ownership": {
        "owners": [
          {
            "owner": {
              "username": "test-user",
              "urn": "urn:li:corpuser:test-user",
              "info": null,
              "editableInfo": null
            },
            "type": "DEVELOPER",
            "source": null
          }
        ],
        "lastModified": {
          "actor": "urn:li:corpuser:datahub"
        }
      }
    }
  }
}
```

### Query DataJob

Request:

```
{
  dataJob(urn: "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow1,foo),task1)") {
    urn
    type
    jobId
    dataFlow {
      urn
      flowId
    }
    inputOutput {
      inputDatasets {
        urn
        name
      }
      outputDatasets {
        urn
        name
      }
    }
  }
}
```

Sample response

```
{
  "data": {
    "dataJob": {
      "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow1,foo),task1)",
      "type": "DATA_JOB",
      "jobId": "task1",
      "dataFlow": {
        "urn": "urn:li:dataFlow:(airflow,flow1,foo)",
        "flowId": "flow1"
      },
      "inputOutput": {
        "inputDatasets": [
          {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:redis,stuff,PROD)",
            "name": "stuff"
          }
        ],
        "outputDatasets": []
      }
    }
  }
}
```
