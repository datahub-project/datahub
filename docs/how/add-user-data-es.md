# Agregar usuario en DataHub

Esta guía comparte cómo puede agregar metadatos de usuario en DataHub. Por lo general, querrá utilizar una de nuestras fuentes para ingerir metadatos de usuario. Pero si no hay un conector para su caso de uso, entonces querrá usar esta guía.

:::nota

Esto no le permite agregar nuevos usuarios para la autenticación. Si desea agregar un nuevo usuario en DataHub para iniciar sesión, consulte [Autenticación JaaS](./auth/jaas.md)

:::

Puede ver todos los aspectos admitidos para los usuarios en [CorpUserAspect](../../metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/CorpUserAspect.pdl)

## Uso de la receta de ingesta basada en archivos

Definir un archivo JSON que contenga el usuario

```my-user.json
[
    {
        "auditHeader": null,
        "proposedSnapshot": {
            "com.linkedin.pegasus2avro.metadata.snapshot.CorpUserSnapshot": {
                "urn": "urn:li:corpuser:aseem.bansal",
                "aspects": [
                    {
                        "com.linkedin.pegasus2avro.identity.CorpUserInfo": {
                            "active": true,
                            "displayName": {
                                "string": "Aseem Bansal"
                            },
                            "email": "aseem+examples@acryl.io",
                            "title": {
                                "string": "Software Engineer"
                            },
                            "managerUrn": null,
                            "departmentId": null,
                            "departmentName": null,
                            "firstName": null,
                            "lastName": null,
                            "fullName": {
                                "string": "Aseem Bansal"
                            },
                            "countryCode": null
                        }
                    }
                ]
            }
        }
    }
]
```

Definir un [receta de ingestión](https://datahubproject.io/docs/metadata-ingestion/#recipes)

```
---
# see https://datahubproject.io/docs/generated/ingestion/sources/file for complete documentation
source:
  type: "file"
  config:
    filename: "./my-user.json"

# see https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for complete documentation
sink:
  ... 

```

Uso [DataHub CLI](../cli.md) para hacer la ingestión.

## Uso de Rest.li API

    curl 'http://localhost:8080/entities?action=ingest' -X POST --data '{
    	"entity": {
    		"value": {
    			"com.linkedin.metadata.snapshot.CorpUserSnapshot": {
    				"urn": "urn:li:corpuser:aseem.bansal",
    				"aspects": [{
    					"com.linkedin.identity.CorpUserInfo": {
                            "active": true, 
    						"displayName": "Aseem Bansal",
    						"email": "aseem+example@acryl.io",
    						"title": "Software Engineer",
    						"fullName": "Aseem Bansal"
    					}
    				}]
    			}
    		}
    	}
    }'
