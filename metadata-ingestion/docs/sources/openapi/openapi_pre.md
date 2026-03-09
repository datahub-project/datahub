### Overview

The `openapi` module ingests metadata from Openapi into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector ingests OpenAPI (Swagger) API endpoint metadata into DataHub. It extracts API endpoints from OpenAPI v2 (Swagger) and v3 specifications and represents them as datasets in DataHub, allowing you to catalog and discover your API endpoints alongside your data assets.

The OpenAPI source extracts metadata from OpenAPI specifications and optionally makes live API calls to gather schema information. It supports:

- **OpenAPI v2 (Swagger) and v3 specifications** - Automatically detects and processes both formats
- **Schema extraction from specifications** - Extracts schemas directly from OpenAPI spec definitions (preferred method)
- **Schema extraction from API calls** - Optionally makes GET requests to endpoints when schema isn't in spec (requires credentials)
- **Multiple HTTP methods** - Supports GET, POST, PUT, and PATCH methods with 200 response codes
- **Browse path organization** - Endpoints are organized by their path structure in DataHub's browse interface
- **Tag extraction** - Preserves tags from OpenAPI specifications
- **Description extraction** - Extracts endpoint descriptions and summaries

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### OpenAPI Specification Access

- The OpenAPI specification file must be accessible via HTTP/HTTPS
- The specification should be in JSON or YAML format
- OpenAPI v2 (Swagger 2.0) and v3.x specifications are supported

#### Authentication (for API calls)

If you want to enable live API calls for schema extraction (`enable_api_calls_for_schema_extraction=True`), you'll need to provide authentication credentials. The source supports:

- **Bearer token authentication**
- **Custom token authentication**
- **Dynamic token retrieval** (GET or POST)
- **Basic authentication** (username/password)

:::note
Authentication is only required if you want to enable live API calls. Schema extraction from the OpenAPI specification itself does not require authentication.
:::
