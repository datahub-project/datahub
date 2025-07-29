# DataHub Demo Dataset Governance Validator

A **demo** custom validator that shows how to ensure all datasets in DataHub have required governance metadata (ownership, tags, and domain) before they can be created or updated. This is intended as an example and starting point for building your own validators.

## Overview

This validator implements DataHub's `AspectPayloadValidator` interface to enforce governance requirements:

- ✅ **Ownership** - At least one owner assigned
- ✅ **Tags** - At least one tag applied
- ✅ **Domain** - Assigned to a business domain

The validator operates at the batch level, validating multiple Metadata Change Proposals (MCPs) together for optimal performance.

## Deployment Options

### Option 1: Custom Docker Image (Recommended)

Build a custom DataHub GMS image with the validator included:

#### 1. Build the Validator

```bash
./gradlew build
```

This creates `build/libs/datahub-dataset-governance-validator-1.0.0.jar`

#### 2. Create Custom GMS Image

Create `Dockerfile`:

```dockerfile
# In this example, we start with the latest open-source DataHub image- this can be any GMS image however.
FROM acryldata/datahub-gms:latest

# Create plugins directory
RUN mkdir -p /etc/datahub/plugins/models/dataset-governance-validator/1.0.0

# Copy validator JAR and configuration
COPY build/libs/datahub-dataset-governance-validator-1.0.0.jar \
     /etc/datahub/plugins/models/dataset-governance-validator/1.0.0/

COPY src/main/resources/entity-registry.yml \
     /etc/datahub/plugins/models/dataset-governance-validator/1.0.0/

# DataHub will auto-discover plugins in this directory
```

#### 3. Build and Deploy

```bash
# Build custom image
docker build -t your-registry/datahub-gms-with-validator:latest .

# Push to registry
docker push your-registry/datahub-gms-with-validator:latest
```

#### 4. Update docker-compose.yml

```yaml
services:
  datahub-gms:
    image: your-registry/datahub-gms-with-validator:latest
    # ... rest of configuration
```

### Option 2: Volume Mount

For development or testing environments, you can mount the validator as a volume:

#### 1. Deploy Plugin Locally

```bash
./gradlew deployPlugin
```

This creates the plugin at `~/.datahub/plugins/models/dataset-governance-validator/1.0.0/`

#### 2. Mount in Docker Compose

```yaml
services:
  datahub-gms:
    image: acryldata/datahub-gms:latest
    volumes:
      - ~/.datahub/plugins:/etc/datahub/plugins/models
    # ... rest of configuration
```

## Kubernetes Deployment

### Using ConfigMap and Volume Mounts

ConfigMap is also a valid strategy, however, it must be noted the size of the validator is limited to the maximum configmap size (1 MiB).

#### 1. Create ConfigMap

```bash
# Create ConfigMap with validator JAR and config
kubectl create configmap dataset-governance-validator \
  --from-file=datahub-dataset-governance-validator-1.0.0.jar=build/libs/datahub-dataset-governance-validator-1.0.0.jar \
  --from-file=entity-registry.yml=src/main/resources/entity-registry.yml
```

#### 2. Update GMS Deployment

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
          image: acryldata/datahub-gms:latest
          volumeMounts:
            - name: validator-plugin
              mountPath: /etc/datahub/plugins/models/dataset-governance-validator/1.0.0
      volumes:
        - name: validator-plugin
          configMap:
            name: dataset-governance-validator
```

### Using Init Container (Alternative)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datahub-gms
spec:
  template:
    spec:
      initContainers:
        - name: install-validator
          image: your-registry/validator-installer:latest
          command: ["cp", "-r", "/validator/", "/shared/plugins/"]
          volumeMounts:
            - name: plugins-volume
              mountPath: /shared/plugins
      containers:
        - name: datahub-gms
          image: acryldata/datahub-gms:latest
          volumeMounts:
            - name: plugins-volume
              mountPath: /etc/datahub/plugins/models
      volumes:
        - name: plugins-volume
          emptyDir: {}
```

## Configuration

### Entity Registry Configuration

The validator is configured via `src/main/resources/entity-registry.yml`:

```yaml
id: "dataset-governance-validator"
entities:
  - name: dataset
    category: core
    aspects:
      - name: schemaMetadata
    plugins:
      aspectPayloadValidators:
        - className: "com.linkedin.metadata.aspect.plugins.validation.DatasetGovernanceValidator"
          supportedOperations: ["CREATE", "UPSERT"]
          supportedEntityAspectNames:
            - entityName: "dataset"
              aspectName: "*"
```

### Customization

To modify validation requirements, edit `DatasetGovernanceValidator.java`:

```java
// Change required aspects
private static final Set<String> REQUIRED_ASPECTS = Set.of(
    Constants.OWNERSHIP_ASPECT_NAME,      // "ownership"
    Constants.GLOBAL_TAGS_ASPECT_NAME,    // "globalTags"
    Constants.DOMAINS_ASPECT_NAME,        // "domains"
    // Add more as needed:
    // Constants.GLOSSARY_TERMS_ASPECT_NAME  // "glossaryTerms"
);
```

## Verification

### Check Plugin Loading

Monitor GMS logs during startup:

```bash
# Docker
docker logs datahub-gms | grep -i "validator\|plugin"

# Kubernetes
kubectl logs deployment/datahub-gms | grep -i "validator\|plugin"
```

Expected output:

```
Enabled 1 plugins. [com.linkedin.metadata.aspect.plugins.validation.DatasetGovernanceValidator]
```

### Test Validation

Use the test script in `LOCAL_DEVELOPER_STEP_BY_STEP_GUIDE.md` or test via REST API:

```bash
# This should fail (missing governance metadata)
curl -X POST "http://your-gms-host:8080/entities?action=ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "entity": {
      "value": {
        "com.linkedin.metadata.snapshot.DatasetSnapshot": {
          "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.test,PROD)",
          "aspects": [{
            "com.linkedin.dataset.DatasetProperties": {
              "name": "test",
              "description": "Test dataset"
            }
          }]
        }
      }
    }
  }'
```

Expected response: HTTP 422 with validation error message.

### Common Issues

**Plugin Not Loading**

- Verify JAR and entity-registry.yml are in correct directory structure
- Check GMS has read permissions on plugin files
- Ensure GMS was restarted after plugin deployment

**Validation Not Triggering**

- Confirm `supportedOperations` includes the operation being performed
- Verify entity type is "dataset" (case-sensitive)
- Check GMS logs for validation exceptions

### Debug Mode

Enable debug logging in GMS configuration:

```yaml
# application.yml
logging:
  level:
    com.linkedin.metadata.aspect.plugins.validation.DatasetGovernanceValidator: DEBUG
```

## Architecture

### Validation Flow

1. **MCP Batch Reception**: GMS receives batch of Metadata Change Proposals
2. **Plugin Invocation**: DataHub calls validator for dataset entities
3. **Dual State Check**: Validator checks both:
   - Existing aspects in GMS database
   - New aspects in current batch
4. **Validation Logic**: Fails if any required aspect missing from both sources
5. **Response**: Returns specific error messages or allows batch to proceed

## File Reference

- `DatasetGovernanceValidator.java` - Main validator implementation
- `entity-registry.yml` - Plugin configuration
- `build.gradle` - Build configuration and deployment tasks
- `LOCAL_DEVELOPER_STEP_BY_STEP_GUIDE.md` - Detailed development setup
- `Dockerfile` - Custom GMS image definition
