This connector ingests OpenAPI (Swagger) API endpoint metadata into DataHub. It extracts API endpoints from OpenAPI v2 (Swagger) and v3 specifications and represents them as datasets in DataHub, allowing you to catalog and discover your API endpoints alongside your data assets.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept         | DataHub Concept                                                                           | Notes                       |
| ---------------------- | ----------------------------------------------------------------------------------------- | --------------------------- |
| `"OpenAPI"`            | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                             |
| API Endpoint           | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            | Subtype `API_ENDPOINT`      |
| Endpoint Path Segments | [Browse Paths](https://docs.datahub.com/docs/generated/metamodel/aspects/browsepaths/)    | Organized by path structure |

## Capabilities

The OpenAPI source extracts metadata from OpenAPI specifications and optionally makes live API calls to gather schema information. It supports:

- **OpenAPI v2 (Swagger) and v3 specifications** - Automatically detects and processes both formats
- **Schema extraction from specifications** - Extracts schemas directly from OpenAPI spec definitions (preferred method)
- **Schema extraction from API calls** - Optionally makes GET requests to endpoints when schema isn't in spec (requires credentials)
- **Multiple HTTP methods** - Supports GET, POST, PUT, and PATCH methods with 200 response codes
- **Browse path organization** - Endpoints are organized by their path structure in DataHub's browse interface
- **Tag extraction** - Preserves tags from OpenAPI specifications
- **Description extraction** - Extracts endpoint descriptions and summaries

## Schema Extraction Behavior

The source uses a multi-step approach to extract schemas for API endpoints:

1. **OpenAPI Specification (Primary)** - The source first attempts to extract schemas directly from the OpenAPI specification file. This includes:

   - Response schemas from 200 responses
   - Request body schemas for POST/PUT/PATCH methods
   - Parameter schemas when available

2. **Example Data (Secondary)** - If schemas aren't fully defined, the source looks for example data in the specification

3. **Live API Calls (Optional)** - If `enable_api_calls_for_schema_extraction=True` and credentials are provided, the source will make GET requests to endpoints when:
   - Schema extraction from the spec fails
   - The endpoint uses the GET method
   - Valid credentials are available (username/password, token, or bearer_token)

:::note
API calls are only made for GET methods. POST, PUT, and PATCH methods rely solely on schema definitions in the OpenAPI specification.
:::

:::tip
Most schemas are extracted from the OpenAPI specification itself. API calls are primarily used as a fallback when the specification is incomplete.
:::

### Schema Extraction Priority

When multiple HTTP methods are available for an endpoint, the source prioritizes extracting metadat from methods in this order:

1. GET
2. POST
3. PUT
4. PATCH

The description, tags, and schema metadata all come from the same priority method to ensure consistency.

## Browse Paths

All ingested endpoints are organized in DataHub's browse interface using browse paths based on their endpoint path structure. This makes it easy to navigate and discover related endpoints.

For example:

- `/pet/findByStatus` appears under the `pet` browse path
- `/pet/{petId}` appears under the `pet` browse path
- `/store/order/{orderId}` appears under `store` → `order`

Endpoints are grouped by their path segments, making it easy to find all endpoints related to a particular resource or feature.

## Prerequisites

### OpenAPI Specification Access

- The OpenAPI specification file must be accessible via HTTP/HTTPS
- The specification should be in JSON or YAML format
- OpenAPI v2 (Swagger 2.0) and v3.x specifications are supported

### Authentication (for API calls)

If you want to enable live API calls for schema extraction (`enable_api_calls_for_schema_extraction=True`), you'll need to provide authentication credentials. The source supports:

- **Bearer token authentication**
- **Custom token authentication**
- **Dynamic token retrieval** (GET or POST)
- **Basic authentication** (username/password)

:::note
Authentication is only required if you want to enable live API calls. Schema extraction from the OpenAPI specification itself does not require authentication.
:::

## Config Details

### Source Configuration

| Field                                    | Required | Description                                                                                                        |
| ---------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------ |
| `name`                                   | Yes      | Name of the ingestion. This appears in DataHub and is used in dataset URNs.                                        |
| `url`                                    | Yes      | Base URL of the API endpoint. e.g. `https://api.example.com`                                                       |
| `swagger_file`                           | Yes      | Path to the OpenAPI specification file relative to the base URL. e.g. `openapi.json` or `api/docs/swagger.yaml`    |
| `ignore_endpoints`                       | No       | List of endpoint paths to exclude from ingestion. e.g. `[/health, /metrics]`                                       |
| `enable_api_calls_for_schema_extraction` | No       | If `True`, makes live GET API calls when schema extraction from spec fails. Default: `True`. Requires credentials. |
| `username`                               | No\*     | Username for basic authentication or token retrieval. Required if using basic auth or `get_token`.                 |
| `password`                               | No\*     | Password for basic authentication or token retrieval. Required if using basic auth or `get_token`.                 |
| `bearer_token`                           | No\*     | Bearer token for authentication. Cannot be used together with `token`.                                             |
| `token`                                  | No\*     | Custom token for authentication. Cannot be used together with `bearer_token`.                                      |
| `get_token`                              | No\*     | Configuration for dynamically retrieving a token. See below for details.                                           |
| `forced_examples`                        | No       | Dictionary mapping endpoint paths to example parameter values. See below for details.                              |
| `proxies`                                | No       | Proxy configuration dictionary. e.g. `{'http': 'http://proxy:8080', 'https': 'https://proxy:8080'}`                |
| `verify_ssl`                             | No       | Enable SSL certificate verification. Default: `True`                                                               |

\* At least one authentication method is required if `enable_api_calls_for_schema_extraction=True`

### Authentication Methods

#### Bearer Token

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    bearer_token: "your-bearer-token-here"
```

#### Custom Token

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    token: "your-token-here"
```

#### Basic Authentication

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    username: your_username
    password: your_password
```

#### Dynamic Token Retrieval

The source can retrieve a token dynamically by making a request to a token endpoint. This is useful when tokens expire and need to be refreshed.

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    get_token:
      request_type: get # or "post"
      url_complement: api/auth/login?username={username}&password={password}
    username: your_username
    password: your_password
```

:::note
When using `get_token` with `request_type: get`, the username and password are sent in the URL query parameters, which is less secure. Use `request_type: post` when possible.
:::

### Forced Examples

For endpoints with path parameters where the source cannot automatically determine example values, you can provide them manually using `forced_examples`:

```yaml
source:
  type: openapi
  config:
    name: petstore_api
    url: https://petstore.swagger.io
    swagger_file: /v2/swagger.json
    forced_examples:
      /pet/{petId}: [1]
      /store/order/{orderId}: [1]
      /user/{username}: ["user1"]
```

The source will use these values to construct URLs for API calls when needed.

### Ignoring Endpoints

You can exclude specific endpoints from ingestion:

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    ignore_endpoints:
      - /health
      - /metrics
      - /internal/debug
```

## Examples

### Basic Configuration (Schema from Spec Only)

```yaml
source:
  type: openapi
  config:
    name: petstore_api
    url: https://petstore.swagger.io
    swagger_file: /v2/swagger.json
    enable_api_calls_for_schema_extraction: false

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

### With API Calls Enabled

```yaml
source:
  type: openapi
  config:
    name: petstore_api
    url: https://petstore.swagger.io
    swagger_file: /v2/swagger.json
    bearer_token: "${BEARER_TOKEN}"
    enable_api_calls_for_schema_extraction: true

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

### Complete Example with All Options

```yaml
source:
  type: openapi
  config:
    name: petstore_api
    url: https://petstore.swagger.io
    swagger_file: /v2/swagger.json

    # Authentication
    bearer_token: "${BEARER_TOKEN}"

    # Optional: Enable/disable API calls
    enable_api_calls_for_schema_extraction: true

    # Optional: Ignore specific endpoints
    ignore_endpoints:
      - /user/logout

    # Optional: Provide example values for parameterized endpoints
    forced_examples:
      /pet/{petId}: [1]
      /store/order/{orderId}: [1]
      /user/{username}: ["user1"]

    # Optional: Proxy configuration
    proxies:
      http: "http://proxy.example.com:8080"
      https: "https://proxy.example.com:8080"

    # Optional: SSL verification
    verify_ssl: true

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

## Limitations

- **API calls are GET-only**: Live API calls for schema extraction are only made for GET methods. POST, PUT, and PATCH methods rely solely on schema definitions in the OpenAPI specification.
- **Authentication required for API calls**: If `enable_api_calls_for_schema_extraction=True`, valid credentials must be provided.
- **200 response codes only**: Only endpoints with 200 response codes are ingested.
- **Schema extraction from spec is preferred**: The source prioritizes extracting schemas from the OpenAPI specification. API calls are used as a fallback.

## Troubleshooting

### No schemas extracted

If schemas aren't being extracted:

1. **Check the OpenAPI specification** - Ensure your spec includes schema definitions in responses or request bodies
2. **Enable API calls** - Set `enable_api_calls_for_schema_extraction: true` and provide credentials
3. **Check authentication** - Verify your credentials are correct if API calls are enabled
4. **Review warnings** - Check the ingestion report for warnings about specific endpoints

### Endpoints not appearing

If endpoints aren't appearing in DataHub:

1. **Check ignore_endpoints** - Ensure endpoints aren't in the ignore list
2. **Verify response codes** - Only endpoints with 200 response codes are ingested
3. **Check OpenAPI spec format** - Ensure the specification is valid OpenAPI v2 or v3

### Authentication errors

If you see authentication errors:

1. **Verify credentials** - Check that username/password or tokens are correct
2. **Check token format** - Bearer tokens should not include the "Bearer " prefix
3. **Review get_token configuration** - Ensure the token endpoint URL and method are correct
