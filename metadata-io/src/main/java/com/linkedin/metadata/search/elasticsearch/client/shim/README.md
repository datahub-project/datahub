# DataHub Search Client Shim

This package provides a shim layer that abstracts different Elasticsearch and OpenSearch client implementations, allowing DataHub to support multiple search engine versions through a common interface.

## Overview

The shim supports the following search engines:

- **Elasticsearch 7.17** - Using REST High Level Client
- **Elasticsearch 8.x** - Using new Elasticsearch Java Client, 8.17+ is supported
- **OpenSearch 2.x** - Using OpenSearch REST High Level Client

## Architecture

### Core Components

1. **`SearchClientShim`** - Main interface that abstracts all search operations
2. **`SearchClientShimFactory`** - Factory for creating appropriate shim implementations
3. **Implementation classes** - Concrete implementations for each search engine type:
   - `Es7CompatibilitySearchClientShim` - ES 7.17
   - `Es8SearchClientShim` - ES 8.17
   - `OpenSearch2SearchClientShim` - OpenSearch 2.x

### Key Features

- **Auto-detection**: Automatically detect the search engine type by connecting to the cluster
- **Configuration-driven**: Select specific client implementations via configuration
- **Backward compatibility**: Existing DataHub code can continue using RestHighLevelClient
- **Feature detection**: Query support for engine-specific features

## Configuration

### Environment Variables

Set these environment variables to configure the shim:

```bash
# Enable the shim (default: false - uses legacy client)
ELASTICSEARCH_SHIM_ENABLED=true

# Specify engine type or use auto-detection
ELASTICSEARCH_SHIM_ENGINE_TYPE=AUTO_DETECT  # or ELASTICSEARCH_7, ELASTICSEARCH_8, OPENSEARCH_2, etc.

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
    apiCompatibilityMode: false
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
    .withEngineType(SearchEngineType.ELASTICSEARCH_7)
    .withHost("localhost")
    .withPort(9200)
    .withCredentials("user", "pass")
    .withApiCompatibilityMode(true)
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

## Migration Guide

## Implementation Status

| Engine Type        | Status      | Client Library                                                  |
| ------------------ | ----------- | --------------------------------------------------------------- |
| Elasticsearch 7.17 | ✅ Complete | `org.elasticsearch.client:elasticsearch-rest-high-level-client` |
| Elasticsearch 8.x  | ✅ Complete | `co.elastic.clients:elasticsearch-java`                         |
| OpenSearch 2.x     | ✅ Complete | `org.opensearch.client:opensearch-rest-high-level-client`       |

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
- Verify cluster is accessible and running
- Review error logs for specific connection issues
