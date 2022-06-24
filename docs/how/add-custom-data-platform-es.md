# Agregar una plataforma de datos de conjunto de datos personalizada

Una plataforma de datos representa un sistema de 3ª parte desde el cual [Entidades de metadatos](https://datahubproject.io/docs/metadata-modeling/metadata-model/) se ingieren de. Cada conjunto de datos que se ingiere está asociado a una sola plataforma, por ejemplo, MySQL, Snowflake, Redshift o BigQuery.

Hay algunos casos en los que es posible que desee agregar un identificador de plataforma de datos personalizado para un conjunto de datos. Por ejemplo
tiene un sistema de datos interno que no está ampliamente disponible o está utilizando una plataforma de datos que no es compatible de forma nativa con DataHub.

Para ello, puede cambiar las plataformas de datos predeterminadas que se ingieren en DataHub *antes de la hora de implementación*, o ingerir
una nueva plataforma de datos en tiempo de ejecución. Puede usar la primera opción si puede combinar periódicamente nuevas plataformas de datos desde el OSS
repositorio en el suyo propio. Hará que la plataforma de datos personalizada se vuelva a ingerir cada vez que implemente DataHub, lo que significa que
su plataforma de datos personalizada persistirá incluso entre limpiezas completas (armas nucleares) de DataHub.

## Cambio de plataformas de datos predeterminadas

Simplemente haga un cambio en el [data_platforms.json](https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json)
para agregar una plataforma de datos personalizada:

    [ 
      .....
      {
        "urn": "urn:li:dataPlatform:MyCustomDataPlatform",
        "aspect": {
          "name": "My Custom Data Platform",
          "type": "OTHERS",
          "logoUrl": "https://<your-logo-url>"
        }
      }
    ]

## Ingestión de Data Platform en tiempo de ejecución

También puede ingerir una plataforma de datos en tiempo de ejecución mediante un origen de ingesta basado en archivos o mediante un rizo normal en el
[API de GMS Rest.li](https://datahubproject.io/docs/metadata-service#restli-api).

### Uso de la receta de ingesta basada en archivos

**Paso 1** Definir un archivo JSON que contenga su plataforma de datos personalizada

    // my-custom-data-platform.json 
    [
      {
        "auditHeader": null,
        "proposedSnapshot": {
          "com.linkedin.pegasus2avro.metadata.snapshot.DataPlatformSnapshot": {
            "urn": "urn:li:dataPlatform:MyCustomDataPlatform",
            "aspects": [
              {
                "com.linkedin.pegasus2avro.dataplatform.DataPlatformInfo": {
                  "datasetNameDelimiter": "/",
                  "name": "My Custom Data Platform",
                  "type": "OTHERS",
                  "logoUrl": "https://<your-logo-url>"
                }
              }
            ]
          }
        },
        "proposedDelta": null
      }
    ]

**Paso 2**: Definir un [receta de ingestión](https://datahubproject.io/docs/metadata-ingestion/#recipes)

    ---
    # see https://datahubproject.io/docs/generated/ingestion/sources/file for complete documentation
    source:
      type: "file"
      config:
        filename: "./my-custom-data-platform.json"

    # see https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for complete documentation
    sink:
      ... 

### Uso de Rest.li API

También puede emitir una solicitud de rizo normal al Rest.li `/entities` API para agregar una plataforma de datos personalizada.

    curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
       "entity":{
          "value":{
             "com.linkedin.metadata.snapshot.DataPlatformSnapshot":{
                "aspects":[
                   {
                      "com.linkedin.dataplatform.DataPlatformInfo":{
                          "datasetNameDelimiter": "/",
                          "name": "My Custom Data Platform",
                          "type": "OTHERS",
                          "logoUrl": "https://<your-logo-url>"
                      }
                   }
                ],
                "urn":"urn:li:dataPlatform:MyCustomDataPlatform"
             }
          }
       }
    }'
