# Adding user metadata in DataHub

This guide shares how you can add user metadata in DataHub. Usually you would want to use one of our sources for ingesting user metadata. But if there is no connector for your use case then you would want to use this guide.

:::note

This does not allow you to add new users for Authentication. If you want to add a new user in DataHub for Login please refer to [Adding Users to DataHub](../authentication/guides/add-users.md)

:::

You can look at all aspects supported for users in [CorpUserAspect](../../metadata-models/src/main/pegasus/com/linkedin/metadata/aspect/CorpUserAspect.pdl)

## Using File-Based Ingestion Recipe

Define a JSON File containing your user

```my-user.json
[
    {
        "entityType": "corpuser",
        "entityUrn": "urn:li:corpuser:aseem.bansal",
        "changeType": "UPSERT",
        "aspectName": "corpUserInfo",
        "aspect": {
            "json": {
                "active": true,
                "displayName": "Aseem Bansal",
                "email": "aseem+examples@acryl.io",
                "title": "Software Engineer",
                "fullName": "Aseem Bansal"
            }
        }
    }
]
```

Define an [ingestion recipe](https://docs.datahub.com/docs/metadata-ingestion/#recipes)

```
---
# see https://docs.datahub.com/docs/generated/ingestion/sources/file for complete documentation
source:
  type: "file"
  config:
    path: "./my-user.json"

# see https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for complete documentation
sink:
 type: "datahub-rest"
  config:
    server: "http://localhost:8080"
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
