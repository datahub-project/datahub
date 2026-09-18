# DataHub Search Client Shim

This package provides a shim layer that abstracts different Elasticsearch and OpenSearch client implementations, allowing DataHub to support multiple search engine versions through a common interface.

## Overview

The shim supports the following search engines:

- **Elasticsearch 8.x / 9.x** - Using the Elasticsearch Java Client (8.17+). Elasticsearch 9.x is detected and served by the same client; it is **not** a certified backend.
- **OpenSearch 2.x / 3.x** - Using the unified OpenSearch low-level RestClient shim

Elasticsearch 7.x is not supported. OpenSearch Elasticsearch compatibility mode (GET / reports 7.10.2) is not supported; disable it so the cluster reports 2.x/3.x.

## Architecture

### Core Components

1. **`SearchClientShim`** - Main interface that abstracts all search operations
2. **`SearchClientShimFactory`** - Factory for creating appropriate shim implementations
3. **Implementation classes** - Concrete implementations for each search engine type:
   - `Es8SearchClientShim` - ES 8.x / 9.x
   - `OpenSearchSearchClientShim` - OpenSearch 2.x / 3.x

### Key Features

- **Auto-detection**: Automatically detect the search engine type by connecting to the cluster
- **Configuration-driven**: Select specific client implementations via configuration
- **Feature detection**: Query support for engine-specific features

## Configuration

### Environment Variables

Set these environment variables to configure the shim:

```bash
# Enable the shim (default: false - uses legacy client)
ELASTICSEARCH_SHIM_ENABLED=true

# Specify engine type or use auto-detection
ELASTICSEARCH_SHIM_ENGINE_TYPE=AUTO_DETECT  # or ELASTICSEARCH_8, ELASTICSEARCH_9, OPENSEARCH_2, OPENSEARCH_3

# Auto-detect engine type (default: true)
ELASTICSEARCH_SHIM_AUTO_DETECT=true
```

### application.yaml Configuration

```yaml
elasticsearch:
  host: localhost
  port: 9200
  # ... other standard config ...
  shim:
    enabled: true
    engineType: AUTO_DETECT
    autoDetectEngine: true
```

## Usage Examples

### Using the Shim Directly

```java
@Autowired
private SearchClientShim searchClientShim;

public void searchExample() throws IOException {
    SearchRequest request = new SearchRequest("my-index");
    SearchResponse response = searchClientShim.search(request, RequestOptions.DEFAULT);

    // Handle response...
}
```

### Creating a Shim Programmatically

```java
SearchClientShim.ShimConfiguration config = new ShimConfigurationBuilder()
    .withEngineType(SearchEngineType.ELASTICSEARCH_8)
    .withHost("localhost")
    .withPort(9200)
    .withCredentials("user", "pass")
    .build();

try (SearchClientShim shim = SearchClientShimFactory.createShim(config)) {
    // Use shim...
}
```

### Auto-Detection

```java
SearchClientShim.ShimConfiguration config = new ShimConfigurationBuilder()
    .withHost("localhost")
    .withPort(9200)
    .build();

// This will auto-detect the engine type
try (SearchClientShim shim = SearchClientShimFactory.createShimWithAutoDetection(config)) {
    SearchEngineType detectedType = shim.getEngineType();
    String version = shim.getEngineVersion();
    System.out.println("Detected: " + detectedType + " version " + version);
}
```

## Implementation Status

| Engine Type        | Status      | Client Library                                            |
| ------------------ | ----------- | --------------------------------------------------------- |
| Elasticsearch 8.x  | ✅ Complete     | `co.elastic.clients:elasticsearch-java`                   |
| Elasticsearch 9.x  | ⚠️ Not certified | Same ES8 Java client; no ES9 CI                           |
| OpenSearch 2.x/3.x | ✅ Complete     | `org.opensearch.client:opensearch-rest-high-level-client` |

## Extending the Shim

To add support for additional search engines:

1. **Implement `SearchClientShim`** for your target client
2. **Add engine type** to `SearchEngineType` enum
3. **Update factory** to create your implementation
4. **Add dependencies** to build.gradle
5. **Update configuration** to support your engine type

## Troubleshooting

### Connection Issues

- Verify host/port configuration
- Check SSL settings
- Ensure credentials are correct
- Look for authentication errors in logs

### Auto-Detection Failures

- Manually specify engine type as fallback
- Check network connectivity to cluster
- Verify cluster is accessible and running a supported version (Elasticsearch 8+ or OpenSearch 2+)
- Review error logs for specific connection issues
