### Prerequisites

In order to ingest metadata from Salesforce, you will need:

- Salesforce username, password, [security token](https://developer.Salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_concepts_security.htm) OR 
- Salesforce instance url and access token/session id (suitable for one-shot ingestion only, as access token typically expires after 2 hours of inactivity)

## Integration Details
This plugin extracts Salesforce Standard and Custom Objects and their details (fields, record count, etc) from a Salesforce instance.
Python library [simple-salesforce](https://pypi.org/project/simple-salesforce/) is used for authenticating and calling  [Salesforce REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm) to retrive details from Salesforce instance.

### REST API Resources used in this integration
- [Versions](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_versions.htm)
- [Tooling API Query](https://developer.salesforce.com/docs/atlas.en-us.api_tooling.meta/api_tooling/intro_rest_resources.htm) on objects EntityDefinition, EntityParticle, CustomObject, CustomField
- [Record Count](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm)

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| `Salesforce` | [Data Platform](../../metamodel/entities/dataPlatform.md) | |
|Standard Object | [Dataset](../../metamodel/entities/dataset.md) | subtype "Standard Object" |
|Custom Object | [Dataset](../../metamodel/entities/dataset.md) | subtype "Custom Object" |

### Caveats
- This connector has only been tested with Salesforce Developer Edition.
- This connector only supports table level profiling (Row and Column counts) as of now. Row counts are approximate as returned by [Salesforce RecordCount REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm).
- This integration does not support ingesting Salesforce [External Objects](https://developer.Salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_external_objects.htm)