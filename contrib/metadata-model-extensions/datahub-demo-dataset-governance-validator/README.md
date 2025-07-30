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
# In this example, we start with the current stable DataHub image- this can be any GMS image however.
FROM acryldata/datahub-gms:v1.2.0

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
# Build custom image (replace 'your-registry' with your Docker registry)
docker build -t your-registry/datahub-gms-with-validator:v1.2.0 .

# Push to registry
docker push your-registry/datahub-gms-with-validator:v1.2.0
```

#### 4. Update docker-compose.yml

```yaml
services:
  datahub-gms:
    image: your-registry/datahub-gms-with-validator:v1.2.0 # Use your custom image
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
    image: acryldata/datahub-gms:v1.2.0
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
          image: acryldata/datahub-gms:v1.2.0
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
          image: acryldata/datahub-gms:v1.2.0
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

Run the Python test script:

```bash
# Set your DataHub token
export DATAHUB_TOKEN="your-token-here"

# Run the test script
python test_validator.py
```

The script will test both scenarios:

1. Dataset without governance metadata (should fail validation)
2. Dataset with complete governance metadata (should succeed)

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
3. **Transaction-Based Validation**: The validator performs batch-level validation, meaning it considers all changes within a single transaction together. This allows for scenarios where multiple aspects are being set simultaneously.
4. **Dual State Check**: For each required aspect, the validator checks:
   - **Existing State**: Whether the aspect already exists in the GMS database
   - **Batch State**: Whether the aspect is being set/updated in the current batch
5. **Validation Logic**:
   - **Pass**: If ANY required aspect exists in either existing state OR batch state
   - **Fail**: If ANY required aspect is missing from BOTH existing state AND batch state
6. **Atomic Response**: The entire batch succeeds or fails as a unit - no partial updates

**Key Benefits of Batch Validation:**

- Allows atomic operations where ownership, tags, and domain can be set together
- Prevents race conditions between multiple simultaneous updates
- Provides consistent validation across related changes
- Supports workflows where governance metadata is added incrementally within a transaction

## File Reference

- `DatasetGovernanceValidator.java` - Main validator implementation
- `entity-registry.yml` - Plugin configuration
- `build.gradle` - Build configuration and deployment tasks
- `test_validator.py` - Python test script for validation testing
- `LOCAL_DEVELOPER_STEP_BY_STEP_GUIDE.md` - Detailed development setup
- `Dockerfile` - Custom GMS image definition
