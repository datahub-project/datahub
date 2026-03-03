# Elasticsearch & OpenSearch Multi-Client Shim

This guide explains how to use DataHub's multi-client search engine shim to support different versions of Elasticsearch and OpenSearch through a unified interface.

## Overview

DataHub's search client shim provides seamless support for:

- **Elasticsearch 7.17**
- **Elasticsearch 8.17+**
- **OpenSearch 2.x** with full REST high-level client support

This enables smooth migrations between different search engine versions while maintaining backward compatibility with existing DataHub deployments.

## Architecture

### Core Components

The shim consists of several key components:

1. **`SearchClientShim`** - Main abstraction interface
2. **`SearchClientShimFactory`** - Factory for creating appropriate client implementations
3. **Implementation Classes** - Concrete implementations for each search engine:
   - `Es7CompatibilitySearchClientShim` - ES 7.17
   - `Es8SearchClientShim` - ES 8.17+
   - `OpenSearch2SearchClientShim` - OpenSearch 2.x

### Supported Configurations

| Source Engine            | Target Engine  | Shim Implementation                | Status      |
| ------------------------ | -------------- | ---------------------------------- | ----------- |
| DataHub → ES 7.17        | ES 7.17        | `Es7CompatibilitySearchClientShim` | ✅ Complete |
| DataHub → ES 8.17+       | ES 8.17+       | `Es8SearchClientShim`              | ✅ Complete |
| DataHub → OpenSearch 2.x | OpenSearch 2.x | `OpenSearch2SearchClientShim`      | ✅ Complete |

## Configuration

### Environment Variables

Configure the shim using these environment variables:

```bash
# Enable the search client shim (required)
ELASTICSEARCH_SHIM_ENABLED=true

# Specify engine type (or use AUTO_DETECT)
ELASTICSEARCH_SHIM_ENGINE_TYPE=AUTO_DETECT
# Options: AUTO_DETECT, ELASTICSEARCH_7, ELASTICSEARCH_8, OPENSEARCH_2

# Enable auto-detection (recommended)
ELASTICSEARCH_SHIM_AUTO_DETECT=true
```

### application.yaml Configuration

Alternatively, configure via application.yaml:

```yaml
elasticsearch:
  host: localhost
  port: 9200
  username: ${ELASTICSEARCH_USERNAME:#{null}}
  password: ${ELASTICSEARCH_PASSWORD:#{null}}
  useSSL: false
  # Standard Elasticsearch configuration...

  # Multi-client shim configuration
  shim:
    enabled: true # Enable shim
    engineType: AUTO_DETECT # or specific type
    autoDetectEngine: true # Auto-detect cluster type
```

## Migration Scenarios

### Scenario 1: Elasticsearch 7.17 → Elasticsearch 8.x

This is the most common migration path.

**Step 1: Enable the shim**

```bash
ELASTICSEARCH_SHIM_ENABLED=true
ELASTICSEARCH_SHIM_ENGINE_TYPE=ELASTICSEARCH_8
```

**Step 2: Verify connection**

```bash
# Check logs for successful connection
```

### Scenario 2: Elasticsearch 7.17 → OpenSearch 2.x

Direct migration from Elasticsearch to OpenSearch 2.x.

**Configuration:**

```bash
ELASTICSEARCH_SHIM_ENABLED=true
ELASTICSEARCH_SHIM_ENGINE_TYPE=OPENSEARCH_2
ELASTICSEARCH_SHIM_AUTO_DETECT=true
```

### Scenario 3: Auto-Detection (Recommended)

Let DataHub automatically detect your search engine type:

```bash
ELASTICSEARCH_SHIM_ENABLED=true
ELASTICSEARCH_SHIM_ENGINE_TYPE=AUTO_DETECT
ELASTICSEARCH_SHIM_AUTO_DETECT=true
```

The shim will:

1. Connect to your search cluster
2. Identify the engine type and version
3. Select the appropriate client implementation

## Deployment Guide

### Docker Compose

Update your `docker-compose.yml`:

```yaml
services:
  datahub-gms:
    environment:
      - ELASTICSEARCH_SHIM_ENABLED=true
      - ELASTICSEARCH_SHIM_ENGINE_TYPE=AUTO_DETECT
      # ... other ES config
```

### Kubernetes

Update your deployment manifests:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datahub-gms
spec:
  template:
    spec:
      containers:
        - name: datahub-gms
          env:
            - name: ELASTICSEARCH_SHIM_ENABLED
              value: "true"
            - name: ELASTICSEARCH_SHIM_ENGINE_TYPE
              value: "AUTO_DETECT"
          # ... other configuration
```

### Helm

Update your `values.yaml`:

```yaml
global:
  elasticsearch:
    shim:
      enabled: true
      engineType: "AUTO_DETECT"
      autoDetectEngine: true
```

## Validation and Testing

### Verify Shim Configuration

1. **Check logs** for shim initialization:

```bash
docker logs datahub-gms | grep -i "shim\|search"
```

Look for messages like:

```
INFO  Creating SearchClientShim for engine type: ELASTICSEARCH_7
INFO  Auto-detected search engine type: ELASTICSEARCH_7
```

1. **Test search functionality** in DataHub UI:

- Search for datasets
- Browse data assets
- Check that lineage is working

2. **Monitor performance** during transition:

- Watch for connection errors
- Check response times
- Monitor resource usage

### Common Validation Steps

```bash
# 1. Check DataHub health endpoint
curl http://localhost:8080/health

# 2. Verify search index access
curl -u user:pass "http://elasticsearch:9200/_cat/indices?v"

# 3. Test search functionality
curl -X POST "http://localhost:8080/api/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ search(input: {type: DATASET, query: \"*\"}) { total }}"}'
```

## Troubleshooting

### Common Issues

#### 1. Connection Failures

```
ERROR: Unable to connect to search cluster
```

**Solutions:**

- Verify `ELASTICSEARCH_HOST` and `ELASTICSEARCH_PORT`
- Check network connectivity between DataHub and search cluster
- Ensure credentials are correct
- Verify SSL/TLS configuration (ES8 Containers use SSL by default so if you previously weren't this may cause issues)

#### 2. Auto-Detection Failures

```
ERROR: Unable to detect search engine type
```

**Solutions:**

- Manually specify engine type: `ELASTICSEARCH_SHIM_ENGINE_TYPE=ELASTICSEARCH_8`
- Check cluster health: `curl http://elasticsearch:9200/_cluster/health`
- Verify authentication credentials

#### 3. API Compatibility Issues

```
ERROR: Incompatible API version
```

**Solutions:**

- Check Elasticsearch version compatibility
- Review deprecation warnings in ES logs

#### 4. Dependency Issues

```
ERROR: ClassNotFoundException for ES client
```

**Solutions:**

- Ensure correct client dependencies are included in classpath
- Check `build.gradle` for required dependencies
- Rebuild DataHub with appropriate client libraries

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
# Add to environment
DATAHUB_LOG_LEVEL=DEBUG
ELASTICSEARCH_SHIM_DEBUG=true
```

### Performance Monitoring

Monitor key metrics during migration:

```bash
# Connection pool metrics
curl "http://localhost:8080/actuator/metrics/elasticsearch.connections"

# Search operation metrics
curl "http://localhost:8080/actuator/metrics/elasticsearch.search"

# Error rates
curl "http://localhost:8080/actuator/metrics/elasticsearch.errors"
```

## Best Practices

### Pre-Migration

1. **Backup your data** before changing search engine configuration
2. **Test in staging** with representative data volumes
3. **Monitor resource usage** patterns in current deployment
4. **Document current configuration** for rollback scenarios

### During Migration

1. **Enable auto-detection initially** for smooth transition
2. **Monitor logs closely** for connection and performance issues
3. **Test all search functionality** after configuration changes

### Post-Migration

1. **Update documentation** with new configuration
2. **Monitor performance metrics** for several days
3. **Plan for future upgrades** (ES 8.x native support)
4. **Train team members** on new configuration options

## Future Enhancements

### Planned Features

1. **OpenSearch 3.x support** when available
2. **Enhanced AWS IAM authentication** for all client types
3. **Advanced feature detection** and capability querying

### Contributing

To extend the shim for additional search engines:

1. **Implement `SearchClientShim`** interface
2. **Add engine type** to `SearchEngineType` enum
3. **Update factory logic** in `SearchClientShimFactory`
4. **Add configuration options** to application.yaml
5. **Write tests** and documentation

## Support Matrix

| DataHub Version | ES 7.17 | ES 8.x   | OpenSearch 2.x |
| --------------- | ------- | -------- | -------------- |
| 0.3.15+         | ✅ Full | ✅ 8.17+ | ✅ Full        |
| Future          | ✅ Full | ✅ Full  | ✅ Full        |

## FAQ

### Q: Can I use the shim with existing deployments?

**A:** Yes, the shim is backward compatible. It is a thin abstraction layer over the existing code

### Q: Can I use multiple search engines simultaneously?

**A:** No, DataHub connects to one search cluster at a time. Use the shim to switch between different engine types.

For additional support, please refer to the [DataHub community forums](https://datahubproject.io/docs/community) or file an issue in the [GitHub repository](https://github.com/datahubproject/datahub).
