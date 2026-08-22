


# OpenAPI

## Overview

OpenAPI is a documentation or collaboration platform. Learn more in the [official OpenAPI documentation](https://www.openapis.org/).

The DataHub integration for OpenAPI covers document/workspace entities and hierarchy context for knowledge assets. It also captures tags.

## Concept Mapping

| Source Concept | DataHub Concept                                                                           | Notes                  |
| -------------- | ----------------------------------------------------------------------------------------- | ---------------------- |
| `"OpenAPI"`    | [Data Platform](/docs/generated/metamodel/entities/dataplatform/) |                        |
| API Endpoint   | [Dataset](/docs/generated/metamodel/entities/dataset/)            | Subtype `API_ENDPOINT` |


## Module `openapi`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Extracts endpoint descriptions and summaries from OpenAPI specifications. |
| [Domains](../../../domains.md) | ❌ | Does not currently support domain assignment. |
| Extract Ownership | ❌ | Does not currently support extracting ownership. |
| Extract Tags | ✅ | Extracts tags from OpenAPI specifications. |
| Schema Metadata | ✅ | Extracts schemas from OpenAPI specifications for GET, POST, PUT, and PATCH methods. |

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


### Install the Plugin
```shell
pip install 'acryl-datahub[openapi]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json

sink:
  type: "datahub-rest"
  config:
    server: 'http://localhost:8080'

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">name</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Name of ingestion.  |
| <div className="path-line"><span className="path-main">swagger_file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Route for access to the swagger file. e.g. openapi.json  |
| <div className="path-line"><span className="path-main">url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Endpoint URL. e.g. https://example.com  |
| <div className="path-line"><span className="path-main">bearer_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Bearer token for endpoint authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">enable_api_calls_for_schema_extraction</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If True, will make live GET API calls to extract schemas when OpenAPI spec extraction fails. Requires credentials (username/password, token, or bearer_token). Only applicable for GET methods. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">forced_examples</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | If no example is provided for a route, it is possible to create one using forced_example. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">get_token</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Retrieving a token from the endpoint. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#125;</span></div> |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Password used for basic HTTP authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">proxies</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Eg. `{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`.If authentication is required, add it to the proxy url directly e.g. `http://user:pass@10.10.1.10:3128/`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">schema_resolution_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth for resolving schema references. Prevents infinite recursion from deeply nested or circular references. Default is 10 levels. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Token for endpoint authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Username used for basic HTTP authentication. <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable SSL certificate verification <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ignore_endpoints</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of endpoints to ignore during ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">ignore_endpoints.</span><span className="path-main">object</span></div> <div className="type-name-line"><span className="type-name">object</span></div> |   |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "additionalProperties": false,
  "description": "Configuration for OpenAPI source ingestion.\n\nThis class defines all the configuration parameters needed to ingest OpenAPI specifications\nand extract dataset metadata from API endpoints.\n\nSchema Extraction Behavior:\n- OpenAPI spec extraction always occurs (parsing the specification file)\n- Example data extraction always occurs (from examples in the spec)\n- Live API calls only occur if enable_api_calls_for_schema_extraction=True\n- API calls are only made for GET methods with valid credentials",
  "properties": {
    "name": {
      "description": "Name of ingestion.",
      "title": "Name",
      "type": "string"
    },
    "url": {
      "description": "Endpoint URL. e.g. https://example.com",
      "title": "Url",
      "type": "string"
    },
    "swagger_file": {
      "description": "Route for access to the swagger file. e.g. openapi.json",
      "title": "Swagger File",
      "type": "string"
    },
    "ignore_endpoints": {
      "default": [],
      "description": "List of endpoints to ignore during ingestion.",
      "items": {},
      "title": "Ignore Endpoints",
      "type": "array"
    },
    "username": {
      "default": "",
      "description": "Username used for basic HTTP authentication.",
      "title": "Username",
      "type": "string"
    },
    "password": {
      "default": "",
      "description": "Password used for basic HTTP authentication.",
      "format": "password",
      "title": "Password",
      "type": "string",
      "writeOnly": true
    },
    "proxies": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Eg. `{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`.If authentication is required, add it to the proxy url directly e.g. `http://user:pass@10.10.1.10:3128/`.",
      "title": "Proxies"
    },
    "forced_examples": {
      "additionalProperties": true,
      "default": {},
      "description": "If no example is provided for a route, it is possible to create one using forced_example.",
      "title": "Forced Examples",
      "type": "object"
    },
    "token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Token for endpoint authentication.",
      "title": "Token"
    },
    "bearer_token": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Bearer token for endpoint authentication.",
      "title": "Bearer Token"
    },
    "get_token": {
      "additionalProperties": true,
      "default": {},
      "description": "Retrieving a token from the endpoint.",
      "title": "Get Token",
      "type": "object"
    },
    "verify_ssl": {
      "default": true,
      "description": "Enable SSL certificate verification",
      "title": "Verify Ssl",
      "type": "boolean"
    },
    "enable_api_calls_for_schema_extraction": {
      "default": true,
      "description": "If True, will make live GET API calls to extract schemas when OpenAPI spec extraction fails. Requires credentials (username/password, token, or bearer_token). Only applicable for GET methods.",
      "title": "Enable Api Calls For Schema Extraction",
      "type": "boolean"
    },
    "schema_resolution_max_depth": {
      "default": 10,
      "description": "Maximum recursion depth for resolving schema references. Prevents infinite recursion from deeply nested or circular references. Default is 10 levels.",
      "title": "Schema Resolution Max Depth",
      "type": "integer"
    }
  },
  "required": [
    "name",
    "url",
    "swagger_file"
  ],
  "title": "OpenApiConfig",
  "type": "object"
}
```





### Capabilities

#### Schema Extraction Behavior

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

#### Schema Extraction Priority

When multiple HTTP methods are available for an endpoint, the source prioritizes extracting metadat from methods in this order:

1. GET
2. POST
3. PUT
4. PATCH

The description, tags, and schema metadata all come from the same priority method to ensure consistency.

#### Browse Paths

All ingested endpoints are organized in DataHub's browse interface using browse paths based on their endpoint path structure. This makes it easy to navigate and discover related endpoints.

For example:

- `/pet/findByStatus` appears under the `pet` browse path
- `/pet/{petId}` appears under the `pet` browse path
- `/store/order/{orderId}` appears under `store` → `order`

Endpoints are grouped by their path segments, making it easy to find all endpoints related to a particular resource or feature.

#### Authentication Methods

##### Bearer Token

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    bearer_token: "your-bearer-token-here"
```

##### Custom Token

```yaml
source:
  type: openapi
  config:
    name: my_api
    url: https://api.example.com
    swagger_file: openapi.json
    token: "your-token-here"
```

##### Basic Authentication

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

##### Dynamic Token Retrieval

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

#### Forced Examples

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

#### Ignoring Endpoints

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

#### Examples

##### Basic Configuration (Schema from Spec Only)

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

##### With API Calls Enabled

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

##### Complete Example with All Options

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

##### No schemas extracted

If schemas aren't being extracted:

1. **Check the OpenAPI specification** - Ensure your spec includes schema definitions in responses or request bodies
2. **Enable API calls** - Set `enable_api_calls_for_schema_extraction: true` and provide credentials
3. **Check authentication** - Verify your credentials are correct if API calls are enabled
4. **Review warnings** - Check the ingestion report for warnings about specific endpoints

### Limitations

- **API calls are GET-only**: Live API calls for schema extraction are only made for GET methods. POST, PUT, and PATCH methods rely solely on schema definitions in the OpenAPI specification.
- **Authentication required for API calls**: If `enable_api_calls_for_schema_extraction=True`, valid credentials must be provided.
- **200 response codes only**: Only endpoints with 200 response codes are ingested.
- **Schema extraction from spec is preferred**: The source prioritizes extracting schemas from the OpenAPI specification. API calls are used as a fallback.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Endpoints not appearing

If endpoints aren't appearing in DataHub:

1. **Check ignore_endpoints** - Ensure endpoints aren't in the ignore list
2. **Verify response codes** - Only endpoints with 200 response codes are ingested
3. **Check OpenAPI spec format** - Ensure the specification is valid OpenAPI v2 or v3

#### Authentication errors

If you see authentication errors:

1. **Verify credentials** - Check that username/password or tokens are correct
2. **Check token format** - Bearer tokens should not include the "Bearer " prefix
3. **Review get_token configuration** - Ensure the token endpoint URL and method are correct


### Code Coordinates
- Class Name: `datahub.ingestion.source.openapi.OpenApiSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/openapi.py)


:::tip Questions?

If you've got any questions on configuring ingestion for OpenAPI, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
