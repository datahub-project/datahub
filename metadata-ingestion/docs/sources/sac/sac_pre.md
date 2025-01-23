## Configuration Notes

1. Refer to [Manage OAuth Clients](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/00f68c2e08b941f081002fd3691d86a7/4f43b54398fc4acaa5efa32badfe3df6.html) to create an OAuth client in SAP Analytics Cloud. The OAuth client is required to have the following properties:

    - Purpose: API Access
    - Access:
        - Story Listing
        - Data Import Service
    - Authorization Grant: Client Credentials

2. Maintain connection mappings (optional):

To map individual connections in SAP Analytics Cloud to platforms, platform instances and environments, the `connection_mapping` configuration can be used within the recipe:

```yaml
connection_mapping:
    MY_BW_CONNECTION:
        platform: bw
        platform_instance: PROD_BW
        env: PROD
    MY_HANA_CONNECTION:
        platform: hana
        platform_instance: PROD_HANA
        env: PROD
```

The key in the connection mapping dictionary represents the name of the connection created in SAP Analytics Cloud.

## Concept mapping

| SAP Analytics Cloud   | DataHub             |
|-----------------------|---------------------|
| `Story`               | `Dashboard`         |
| `Application`         | `Dashboard`         |
| `Live Data Model`     | `Dataset`           |
| `Import Data Model`   | `Dataset`           |
| `Model`               | `Dataset`           |

## Limitations

- Only models which are used in a Story or an Application will be ingested because there is no dedicated API to retrieve models (only for Stories and Applications).
- Browse Paths for models cannot be created because the folder where the models are saved is not returned by the API.
- Schema metadata is only ingested for Import Data Models because there is no possibility to get the schema metadata of the other model types.
- Lineages for Import Data Models cannot be ingested because the API is not providing any information about it.
- Currently, only SAP BW and SAP HANA are supported for ingesting the upstream lineages of Live Data Models - a warning is logged for all other connection types, please feel free to open an [issue on GitHub](https://github.com/datahub-project/datahub/issues/new/choose) with the warning message to have this fixed.
- For some models (e.g., builtin models) it cannot be detected whether the models are Live Data or Import Data Models. Therefore, these models will be ingested only with the `Story` subtype.
