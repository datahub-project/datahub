# Empezar

## Introducción a OpenAPI - Qué y por qué

El estándar OpenAPI es un enfoque de documentación y diseño ampliamente utilizado para las API. Históricamente, hemos publicado nuestras API RESTful
bajo el estándar Rest.li que es utilizado internamente por LinkedIn que heredamos desde el inicio del proyecto.

Rest.li es un marco muy obstinado que no ha visto una amplia adopción en la comunidad de código abierto, por lo que los usuarios a menudo no están familiarizados.
con las mejores formas de interactuar con estos endpoints. Para facilitar la integración con DataHub, estamos publicando un conjunto de puntos finales basado en OpenAPI.

## Uso de OpenAPI de DataHub: dónde y cómo

Actualmente, los puntos finales de OpenAPI están aislados en un servlet en GMS y se implementan automáticamente con un servidor GMS.
El servlet incluye la generación automática de una interfaz de usuario OpenAPI, también conocida como Swagger, que está disponible en `<GMS-server-host>:<GMS-port>/openapi/swagger-ui/index.html`.
Esto también se expone a través del frontend de DataHub como un proxy con el mismo punto final, pero el host y el puerto de GMS se reemplazan con la url del frontend de DataHub.
y está disponible en el menú desplegable superior derecho debajo de la foto de perfil del usuario como un enlace. Tenga en cuenta que es posible obtener
Los formatos JSON o YAML sin procesar de la especificación OpenAPI navegando a `<baseUrl>/openapi/v3/api-docs` o `<baseUrl>/openapi/v3/api-docs.yaml`.
Los formularios sin procesar se pueden alimentar a los sistemas codegen para generar código del lado del cliente en el lenguaje de su elección que admita el formato OpenAPI. Hemos notado variaciones
grados de madurez con diferentes lenguajes en estos sistemas de códices por lo que algunos pueden requerir personalizaciones para ser totalmente compatibles.

La interfaz de usuario de OpenAPI incluye esquemas explorables para objetos de solicitud y respuesta que están completamente documentados. Los modelos utilizados
en la interfaz de usuario de OpenAPI se generan automáticamente en el momento de la compilación desde los modelos PDL hasta los modelos Java compatibles con el esquema JSON.

El uso programático de los modelos se puede hacer a través del Java Rest Emitter que incluye los modelos generados. Un mínimo
El proyecto Java para emitir a los puntos finales de OpenAPI necesitaría las siguientes dependencias (formato gradle):

```groovy
dependencies {
    implementation 'io.acryl:datahub-client:<DATAHUB_CLIENT_VERSION>'
    implementation 'org.apache.httpcomponents:httpclient:<APACHE_HTTP_CLIENT_VERSION>'
    implementation 'org.apache.httpcomponents:httpasyncclient:<APACHE_ASYNC_CLIENT_VERSION>'
}
```

y construiría una lista de `UpsertAspectRequest`s para emitir:

```java
import io.datahubproject.openapi.generated.DatasetProperties;
import datahub.client.rest.RestEmitter;
import datahub.event.UpsertAspectRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class Main {
  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    RestEmitter emitter = RestEmitter.createWithDefaults();

    List<UpsertAspectRequest> requests = new ArrayList<>();
    UpsertAspectRequest upsertAspectRequest = UpsertAspectRequest.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-other-dataset.user-table,PROD)")
        .aspect(new DatasetProperties().description("This is the canonical User profile dataset"))
        .build();
    UpsertAspectRequest upsertAspectRequest2 = UpsertAspectRequest.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.another-dataset.user-table,PROD)")
        .aspect(new DatasetProperties().description("This is the canonical User profile dataset 2"))
        .build();
    requests.add(upsertAspectRequest);
    requests.add(upsertAspectRequest2);
    System.out.println(emitter.emit(requests, null).get());
    System.exit(0);
  }
}
```

### Solicitudes de ejemplo

#### Rizo

##### EXPONER

```shell
curl --location --request POST 'localhost:8080/openapi/entities/v1/' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY1MDY2MDY1NSwianRpIjoiM2E4ZDY3ZTItOTM5Yi00NTY3LWE0MjYtZDdlMDA1ZGU3NjJjIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.pp_vW2u1tiiTT7U0nDF2EQdcayOMB8jatiOA8Je4JJA' \
--data-raw '[
    {
        "aspect": {
            "__type": "SchemaMetadata",
            "schemaName": "SampleHdfsSchema",
            "platform": "urn:li:dataPlatform:platform",
            "platformSchema": {
                "__type": "MySqlDDL",
                "tableSchema": "schema"
            },
            "version": 0,
            "created": {
                "time": 1621882982738,
                "actor": "urn:li:corpuser:etl",
                "impersonator": "urn:li:corpuser:jdoe"
            },
            "lastModified": {
                "time": 1621882982738,
                "actor": "urn:li:corpuser:etl",
                "impersonator": "urn:li:corpuser:jdoe"
            },
            "hash": "",
            "fields": [
                {
                    "fieldPath": "county_fips_codefg",
                    "jsonPath": "null",
                    "nullable": true,
                    "description": "null",
                    "type": {
                        "type": {
                            "__type": "StringType"
                        }
                    },
                    "nativeDataType": "String()",
                    "recursive": false
                },
                {
                    "fieldPath": "county_name",
                    "jsonPath": "null",
                    "nullable": true,
                    "description": "null",
                    "type": {
                        "type": {
                            "__type": "StringType"
                        }
                    },
                    "nativeDataType": "String()",
                    "recursive": false
                }
            ]
        },
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)"
    }
]'
```

##### OBTENER

```shell
curl --location --request GET 'localhost:8080/openapi/entities/v1/latest?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&aspectNames=schemaMetadata' \
--header 'Accept: application/json' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY1MDY2MDY1NSwianRpIjoiM2E4ZDY3ZTItOTM5Yi00NTY3LWE0MjYtZDdlMDA1ZGU3NjJjIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.pp_vW2u1tiiTT7U0nDF2EQdcayOMB8jatiOA8Je4JJA'
```

##### BORRAR

```shell
curl --location --request DELETE 'localhost:8080/openapi/entities/v1/?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&soft=true' \
--header 'Accept: application/json' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY1MDY2MDY1NSwianRpIjoiM2E4ZDY3ZTItOTM5Yi00NTY3LWE0MjYtZDdlMDA1ZGU3NjJjIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.pp_vW2u1tiiTT7U0nDF2EQdcayOMB8jatiOA8Je4JJA'
```

#### Colección Cartero

La colección incluye POST, GET y DELETE para una sola entidad con un aspecto SchemaMetadata

```json
{
  "info": {
    "_postman_id": "87b7401c-a5dc-47e4-90b4-90fe876d6c28",
    "name": "DataHub OpenAPI",
    "description": "A description",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "entities/v1",
      "item": [
        {
          "name": "post Entities 1",
          "request": {
            "method": "POST",
            "header": [
              {
                "key": "Content-Type",
                "value": "application/json"
              },
              {
                "key": "Accept",
                "value": "application/json"
              }
            ],
            "body": {
              "mode": "raw",
              "raw": "[\n    {\n        \"aspect\": {\n            \"__type\": \"SchemaMetadata\",\n            \"schemaName\": \"SampleHdfsSchema\",\n            \"platform\": \"urn:li:dataPlatform:platform\",\n            \"platformSchema\": {\n                \"__type\": \"MySqlDDL\",\n                \"tableSchema\": \"schema\"\n            },\n            \"version\": 0,\n            \"created\": {\n                \"time\": 1621882982738,\n                \"actor\": \"urn:li:corpuser:etl\",\n                \"impersonator\": \"urn:li:corpuser:jdoe\"\n            },\n            \"lastModified\": {\n                \"time\": 1621882982738,\n                \"actor\": \"urn:li:corpuser:etl\",\n                \"impersonator\": \"urn:li:corpuser:jdoe\"\n            },\n            \"hash\": \"\",\n            \"fields\": [\n                {\n                    \"fieldPath\": \"county_fips_codefg\",\n                    \"jsonPath\": \"null\",\n                    \"nullable\": true,\n                    \"description\": \"null\",\n                    \"type\": {\n                        \"type\": {\n                            \"__type\": \"StringType\"\n                        }\n                    },\n                    \"nativeDataType\": \"String()\",\n                    \"recursive\": false\n                },\n                {\n                    \"fieldPath\": \"county_name\",\n                    \"jsonPath\": \"null\",\n                    \"nullable\": true,\n                    \"description\": \"null\",\n                    \"type\": {\n                        \"type\": {\n                            \"__type\": \"StringType\"\n                        }\n                    },\n                    \"nativeDataType\": \"String()\",\n                    \"recursive\": false\n                }\n            ]\n        },\n        \"aspectName\": \"schemaMetadata\",\n        \"entityType\": \"dataset\",\n        \"entityUrn\": \"urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)\"\n    }\n]",
              "options": {
                "raw": {
                  "language": "json"
                }
              }
            },
            "url": {
              "raw": "{{baseUrl}}/openapi/entities/v1/",
              "host": [
                "{{baseUrl}}"
              ],
              "path": [
                "openapi",
                "entities",
                "v1",
                ""
              ]
            }
          },
          "response": [
            {
              "name": "OK",
              "originalRequest": {
                "method": "POST",
                "header": [],
                "body": {
                  "mode": "raw",
                  "raw": "[\n  {\n    \"aspect\": {\n      \"value\": \"<Error: Too many levels of nesting to fake this schema>\"\n    },\n    \"aspectName\": \"aliquip ipsum tempor\",\n    \"entityType\": \"ut est\",\n    \"entityUrn\": \"enim in nulla\",\n    \"entityKeyAspect\": {\n      \"value\": \"<Error: Too many levels of nesting to fake this schema>\"\n    }\n  },\n  {\n    \"aspect\": {\n      \"value\": \"<Error: Too many levels of nesting to fake this schema>\"\n    },\n    \"aspectName\": \"ipsum id\",\n    \"entityType\": \"deser\",\n    \"entityUrn\": \"aliqua sit\",\n    \"entityKeyAspect\": {\n      \"value\": \"<Error: Too many levels of nesting to fake this schema>\"\n    }\n  }\n]",
                  "options": {
                    "raw": {
                      "language": "json"
                    }
                  }
                },
                "url": {
                  "raw": "{{baseUrl}}/entities/v1/",
                  "host": [
                    "{{baseUrl}}"
                  ],
                  "path": [
                    "entities",
                    "v1",
                    ""
                  ]
                }
              },
              "status": "OK",
              "code": 200,
              "_postman_previewlanguage": "json",
              "header": [
                {
                  "key": "Content-Type",
                  "value": "application/json"
                }
              ],
              "cookie": [],
              "body": "[\n  \"c\",\n  \"labore dolor exercitation in\"\n]"
            }
          ]
        },
        {
          "name": "delete Entities",
          "request": {
            "method": "DELETE",
            "header": [
              {
                "key": "Accept",
                "value": "application/json"
              }
            ],
            "url": {
              "raw": "{{baseUrl}}/openapi/entities/v1/?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&soft=true",
              "host": [
                "{{baseUrl}}"
              ],
              "path": [
                "openapi",
                "entities",
                "v1",
                ""
              ],
              "query": [
                {
                  "key": "urns",
                  "value": "urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)",
                  "description": "(Required) A list of raw urn strings, only supports a single entity type per request."
                },
                {
                  "key": "urns",
                  "value": "labore dolor exercitation in",
                  "description": "(Required) A list of raw urn strings, only supports a single entity type per request.",
                  "disabled": true
                },
                {
                  "key": "soft",
                  "value": "true",
                  "description": "Determines whether the delete will be soft or hard, defaults to true for soft delete"
                }
              ]
            }
          },
          "response": [
            {
              "name": "OK",
              "originalRequest": {
                "method": "DELETE",
                "header": [],
                "url": {
                  "raw": "{{baseUrl}}/entities/v1/?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&soft=true",
                  "host": [
                    "{{baseUrl}}"
                  ],
                  "path": [
                    "entities",
                    "v1",
                    ""
                  ],
                  "query": [
                    {
                      "key": "urns",
                      "value": "urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)"
                    },
                    {
                      "key": "urns",
                      "value": "officia occaecat elit dolor",
                      "disabled": true
                    },
                    {
                      "key": "soft",
                      "value": "true"
                    }
                  ]
                }
              },
              "status": "OK",
              "code": 200,
              "_postman_previewlanguage": "json",
              "header": [
                {
                  "key": "Content-Type",
                  "value": "application/json"
                }
              ],
              "cookie": [],
              "body": "[\n    {\n        \"rowsRolledBack\": [\n            {\n                \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)\"\n            }\n        ],\n        \"rowsDeletedFromEntityDeletion\": 1\n    }\n]"
            }
          ]
        },
        {
          "name": "get Entities",
          "protocolProfileBehavior": {
            "disableUrlEncoding": false
          },
          "request": {
            "method": "GET",
            "header": [
              {
                "key": "Accept",
                "value": "application/json"
              }
            ],
            "url": {
              "raw": "{{baseUrl}}/openapi/entities/v1/latest?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&aspectNames=schemaMetadata",
              "host": [
                "{{baseUrl}}"
              ],
              "path": [
                "openapi",
                "entities",
                "v1",
                "latest"
              ],
              "query": [
                {
                  "key": "urns",
                  "value": "urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)",
                  "description": "(Required) A list of raw urn strings, only supports a single entity type per request."
                },
                {
                  "key": "urns",
                  "value": "labore dolor exercitation in",
                  "description": "(Required) A list of raw urn strings, only supports a single entity type per request.",
                  "disabled": true
                },
                {
                  "key": "aspectNames",
                  "value": "schemaMetadata",
                  "description": "The list of aspect names to retrieve"
                },
                {
                  "key": "aspectNames",
                  "value": "labore dolor exercitation in",
                  "description": "The list of aspect names to retrieve",
                  "disabled": true
                }
              ]
            }
          },
          "response": [
            {
              "name": "OK",
              "originalRequest": {
                "method": "GET",
                "header": [],
                "url": {
                  "raw": "{{baseUrl}}/entities/v1/latest?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&aspectNames=schemaMetadata",
                  "host": [
                    "{{baseUrl}}"
                  ],
                  "path": [
                    "entities",
                    "v1",
                    "latest"
                  ],
                  "query": [
                    {
                      "key": "urns",
                      "value": "non exercitation occaecat",
                      "disabled": true
                    },
                    {
                      "key": "urns",
                      "value": "urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)"
                    },
                    {
                      "key": "aspectNames",
                      "value": "non exercitation occaecat",
                      "disabled": true
                    },
                    {
                      "key": "aspectNames",
                      "value": "schemaMetadata"
                    }
                  ]
                }
              },
              "status": "OK",
              "code": 200,
              "_postman_previewlanguage": "json",
              "header": [
                {
                  "key": "Content-Type",
                  "value": "application/json"
                }
              ],
              "cookie": [],
              "body": "{\n    \"responses\": {\n        \"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\": {\n            \"entityName\": \"dataset\",\n            \"urn\": \"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",\n            \"aspects\": {\n                \"datasetKey\": {\n                    \"name\": \"datasetKey\",\n                    \"type\": \"VERSIONED\",\n                    \"version\": 0,\n                    \"value\": {\n                        \"__type\": \"DatasetKey\",\n                        \"platform\": \"urn:li:dataPlatform:hive\",\n                        \"name\": \"SampleHiveDataset\",\n                        \"origin\": \"PROD\"\n                    },\n                    \"created\": {\n                        \"time\": 1650657843351,\n                        \"actor\": \"urn:li:corpuser:__datahub_system\"\n                    }\n                },\n                \"schemaMetadata\": {\n                    \"name\": \"schemaMetadata\",\n                    \"type\": \"VERSIONED\",\n                    \"version\": 0,\n                    \"value\": {\n                        \"__type\": \"SchemaMetadata\",\n                        \"schemaName\": \"SampleHiveSchema\",\n                        \"platform\": \"urn:li:dataPlatform:hive\",\n                        \"version\": 0,\n                        \"created\": {\n                            \"time\": 1581407189000,\n                            \"actor\": \"urn:li:corpuser:jdoe\"\n                        },\n                        \"lastModified\": {\n                            \"time\": 1581407189000,\n                            \"actor\": \"urn:li:corpuser:jdoe\"\n                        },\n                        \"hash\": \"\",\n                        \"platformSchema\": {\n                            \"__type\": \"KafkaSchema\",\n                            \"documentSchema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"SampleHiveSchema\\\",\\\"namespace\\\":\\\"com.linkedin.dataset\\\",\\\"doc\\\":\\\"Sample Hive dataset\\\",\\\"fields\\\":[{\\\"name\\\":\\\"field_foo\\\",\\\"type\\\":[\\\"string\\\"]},{\\\"name\\\":\\\"field_bar\\\",\\\"type\\\":[\\\"boolean\\\"]}]}\"\n                        },\n                        \"fields\": [\n                            {\n                                \"fieldPath\": \"field_foo\",\n                                \"nullable\": false,\n                                \"description\": \"Foo field description\",\n                                \"type\": {\n                                    \"type\": {\n                                        \"__type\": \"BooleanType\"\n                                    }\n                                },\n                                \"nativeDataType\": \"varchar(100)\",\n                                \"recursive\": false,\n                                \"isPartOfKey\": true\n                            },\n                            {\n                                \"fieldPath\": \"field_bar\",\n                                \"nullable\": false,\n                                \"description\": \"Bar field description\",\n                                \"type\": {\n                                    \"type\": {\n                                        \"__type\": \"BooleanType\"\n                                    }\n                                },\n                                \"nativeDataType\": \"boolean\",\n                                \"recursive\": false,\n                                \"isPartOfKey\": false\n                            }\n                        ]\n                    },\n                    \"created\": {\n                        \"time\": 1650610810000,\n                        \"actor\": \"urn:li:corpuser:UNKNOWN\"\n                    }\n                }\n            }\n        }\n    }\n}"
            }
          ]
        }
      ],
      "auth": {
        "type": "bearer",
        "bearer": [
          {
            "key": "token",
            "value": "{{token}}",
            "type": "string"
          }
        ]
      },
      "event": [
        {
          "listen": "prerequest",
          "script": {
            "type": "text/javascript",
            "exec": [
              ""
            ]
          }
        },
        {
          "listen": "test",
          "script": {
            "type": "text/javascript",
            "exec": [
              ""
            ]
          }
        }
      ]
    }
  ],
  "event": [
    {
      "listen": "prerequest",
      "script": {
        "type": "text/javascript",
        "exec": [
          ""
        ]
      }
    },
    {
      "listen": "test",
      "script": {
        "type": "text/javascript",
        "exec": [
          ""
        ]
      }
    }
  ],
  "variable": [
    {
      "key": "baseUrl",
      "value": "localhost:8080",
      "type": "string"
    },
    {
      "key": "token",
      "value": "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMSIsImV4cCI6MTY1MDY2MDY1NSwianRpIjoiM2E4ZDY3ZTItOTM5Yi00NTY3LWE0MjYtZDdlMDA1ZGU3NjJjIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.pp_vW2u1tiiTT7U0nDF2EQdcayOMB8jatiOA8Je4JJA",
      "type": "default"
    }
  ]
}
```
