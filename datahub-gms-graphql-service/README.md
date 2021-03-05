# DataHub GMS GraphQL Service

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

Before starting `Datahub GMS GraphQL Service`, you need to make sure that [DataHub GMS](../gms/README.md) is up and running.

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

