### Overview

The `salesforce` module ingests metadata from Salesforce into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

### Integration Details

This plugin extracts Salesforce Standard and Custom Objects and their details (fields, record count, etc) from a Salesforce instance.
Python library [simple-salesforce](https://pypi.org/project/simple-salesforce/) is used for authenticating and calling [Salesforce REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm) to retrieve details from Salesforce instance.

#### REST API Resources used in this integration

- [Versions](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_versions.htm)
- [Tooling API Query](https://developer.salesforce.com/docs/atlas.en-us.api_tooling.meta/api_tooling/intro_rest_resources.htm) on objects EntityDefinition, EntityParticle, CustomObject, CustomField
- [Record Count](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm)

#### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept  | DataHub Concept                                           | Notes                     |
| --------------- | --------------------------------------------------------- | ------------------------- |
| `Salesforce`    | [Data Platform](../../metamodel/entities/dataPlatform.md) |                           |
| Standard Object | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Standard Object" |
| Custom Object   | [Dataset](../../metamodel/entities/dataset.md)            | subtype "Custom Object"   |

#### Caveats

- This connector has only been tested with Salesforce Developer Edition.
- This connector only supports table level profiling (Row and Column counts) as of now. Row counts are approximate as returned by [Salesforce RecordCount REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm).
- This integration does not support ingesting Salesforce [External Objects](https://developer.Salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_external_objects.htm)
