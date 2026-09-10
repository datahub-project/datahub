### Overview

The `cube` module ingests the [Cube](https://cube.dev/) semantic layer data model into DataHub. Every cube and view is emitted as a dataset, with its measures and dimensions modelled as schema fields, organised under a container that represents the Cube deployment. The module works against both Cube Core and Cube Cloud.

### Prerequisites

#### Choose a deployment type

Set `deployment_type` to match your Cube installation:

- `CORE` — a self-hosted [Cube Core](https://cube.dev/docs/product/deployment/core) instance. Metadata is read from the `/v1/meta` REST endpoint.
- `CLOUD` — a [Cube Cloud](https://cube.dev/docs/product/deployment/cloud) deployment. When `use_metadata_api` is enabled, the connector reads from the [Metadata API](https://cube.dev/docs/product/apis-integrations/core-data-apis/rest-api/reference#metadata-api), which additionally exposes lineage to upstream warehouse tables. If the supplied token lacks the required scope, the connector automatically falls back to `/v1/meta`.

#### Obtain an API token

The connector authenticates with a token sent in the `Authorization` header.

- **Cube Core**: generate a JWT signed with your deployment's `CUBEJS_API_SECRET`. See [Security context](https://cube.dev/docs/product/auth).
- **Cube Cloud** (`/v1/meta`): copy a token from the deployment's **Playground → API** tab, or sign one with the deployment's API secret.
- **Cube Cloud Metadata API**: obtain a token via the [Control Plane API](https://cube.dev/docs/product/apis-integrations/control-plane-api). This token is required for warehouse lineage.

#### Configure the API URL

`api_url` is the base URL of the REST API, including the base path (defaults to `/cubejs-api`):

- Cube Core: `http://localhost:4000/cubejs-api`
- Cube Cloud: `https://<deployment>.cubecloud.dev/cubejs-api`

#### Warehouse lineage (optional)

To connect cubes to the warehouse tables they read from, set `warehouse_platform` (e.g. `snowflake`, `bigquery`, `postgres`) and, if your existing datasets use them, `warehouse_platform_instance` and `warehouse_env`. On Cube Cloud with the Metadata API enabled, the warehouse platform and database are auto-detected from the deployment's data sources. On Cube Core, set `parse_sql_for_lineage` to derive table lineage from each cube's SQL definition (requires `warehouse_platform`).

Note that cubes marked `public: false` are not returned by the `/v1/meta` endpoint, so views that reference them will still produce lineage edges to those cubes even though the cubes themselves are not ingested.
