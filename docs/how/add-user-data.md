# Adding user in DataHub

This guide shares how you can add user metadata in DataHub. Usually you would want to use one of our sources for ingesting user metadata. But if there is no connector for your use case then you would want to use this guide.

:::note

This does not allow you to add new users for Authentication. If you want to add a new user in DataHub for Login please refer to [JaaS Authentication](./auth/jaas.md)

:::

You can look at all aspects supported for users in [CorpUserAspect](../../metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/CorpUserAspect.pdl)

## Using File-Based Ingestion Recipe

Define a JSON File containing your user
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

Define an [ingestion recipe](https://datahubproject.io/docs/metadata-ingestion/#recipes) 

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

Use [DataHub CLI](../cli.md) to do the ingestion.

## Using Rest.li API

```
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
```

