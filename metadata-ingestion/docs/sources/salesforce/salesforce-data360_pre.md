### Overview

The `salesforce-data360` module ingests the data model of Salesforce Data 360 (formerly Data Cloud). It reads the Data 360 Connect REST API and emits each Data Model Object (DMO, API names ending in `__dlm`) as a dataset with its fields, primary keys and DMO-to-DMO relationships as foreign keys. It reads metadata only, never record data.

By default, it ingests enabled DMOs and all custom DMOs. A new org lists about a thousand standard DMOs that are not enabled; set `include_disabled_dmos: true` to ingest them too. Only active relationships become foreign keys.

##### REST API resources used in this integration

- [Versions](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_versions.htm)
- `GET /services/data/vXX.X/ssot/data-model-objects`
- `GET /services/data/vXX.X/ssot/data-model-objects/{name}/relationships`

### Prerequisites

The module uses the same authentication options as the `salesforce` module: username, password and security token, a JSON web token, or an instance URL with an access token. The user needs access to Data 360 in the org and the API Enabled permission.
