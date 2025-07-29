# Local Developer Step-by-Step Guide

## Step 1: Create the Validator Project

First, create a new Gradle project structure:

```bash
mkdir datahub-dataset-governance-validator
cd datahub-dataset-governance-validator
```

## Step 2: Set Up Build Configuration

Create `build.gradle` with the following content:

```gradle
plugins {
    id 'java'
}

group = 'com.linkedin.metadata'
version = '1.0.0'

repositories {
    mavenCentral()
    maven {
        url "https://linkedin.jfrog.io/artifactory/open-source/"
    }
}

dependencies {
    implementation 'io.acryl:datahub-entity-registry:0.13.2'
    implementation 'com.linkedin.datahub:datahub-metadata-models:0.13.2'
    implementation 'org.slf4j:slf4j-api:1.7.36'

    compileOnly 'org.projectlombok:lombok:1.18.24'
    annotationProcessor 'org.projectlombok:lombok:1.18.24'
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

// Create model plugin structure
task createModelPlugin(type: Copy) {
    dependsOn jar
    from jar
    from 'src/main/resources/entity-registry.yml'
    into "$buildDir/model-plugin"
}

// Deploy to DataHub plugins directory
task deployPlugin(type: Copy) {
    dependsOn createModelPlugin
    from "$buildDir/model-plugin"
    into "${System.properties['user.home']}/.datahub/plugins/models/dataset-governance-validator/1.0.0"
}
```

## Step 3: Implement the Validator

Create the directory structure:

```bash
mkdir -p src/main/java/com/linkedin/metadata/aspect/plugins/validation
```

Create `src/main/java/com/linkedin/metadata/aspect/plugins/validation/DatasetGovernanceValidator.java`:

```java
package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom validator that ensures dataset URNs have ownership, tags, and domain set
 * either in the current batch of MCPs or already existing in GMS.
 */
public class DatasetGovernanceValidator extends AspectPayloadValidator {

  private static final Logger logger = LoggerFactory.getLogger(DatasetGovernanceValidator.class);

  private static final String DATASET_ENTITY_TYPE = "dataset";
  private static final Set<String> REQUIRED_ASPECTS = Set.of(
      Constants.OWNERSHIP_ASPECT_NAME,      // "ownership"
      Constants.GLOBAL_TAGS_ASPECT_NAME,    // "globalTags"
      Constants.DOMAINS_ASPECT_NAME         // "domains"
  );

  private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    logger.debug("Validating {} MCP items", mcpItems.size());

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // Group all dataset URNs in the batch
    Set<Urn> datasetUrns = mcpItems.stream()
        .map(BatchItem::getUrn)
        .filter(urn -> DATASET_ENTITY_TYPE.equals(urn.getEntityType()))
        .collect(Collectors.toSet());

    if (datasetUrns.isEmpty()) {
      return Stream.empty();
    }

    // Create map for batch fetching existing aspects
    Map<Urn, Set<String>> urnAspects = datasetUrns.stream()
        .collect(Collectors.toMap(
            urn -> urn,
            urn -> REQUIRED_ASPECTS
        ));

    // Batch fetch existing aspects from GMS
    Map<Urn, Map<String, SystemAspect>> existingAspects =
        aspectRetriever.getLatestSystemAspects(urnAspects);

    // Track which aspects are being set in this batch
    Map<Urn, Set<String>> batchAspects = mcpItems.stream()
        .filter(item -> DATASET_ENTITY_TYPE.equals(item.getUrn().getEntityType()))
        .filter(item -> REQUIRED_ASPECTS.contains(item.getAspectName()))
        .collect(Collectors.groupingBy(
            BatchItem::getUrn,
            Collectors.mapping(BatchItem::getAspectName, Collectors.toSet())
        ));

    // Validate each dataset URN
    return datasetUrns.stream()
        .map(urn -> validateDatasetGovernance(urn, existingAspects.get(urn),
                                              batchAspects.get(urn), mcpItems))
        .filter(Objects::nonNull);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {

    // Use same validation logic for pre-commit
    return validateProposedAspects(changeMCPs, retrieverContext);
  }

  private AspectValidationException validateDatasetGovernance(
      Urn datasetUrn,
      Map<String, SystemAspect> existingAspects,
      Set<String> batchAspects,
      Collection<? extends BatchItem> mcpItems) {

    Set<String> missingAspects = REQUIRED_ASPECTS.stream()
        .filter(aspectName -> {
          boolean existsInGMS = existingAspects != null &&
                               existingAspects.containsKey(aspectName);
          boolean existsInBatch = batchAspects != null &&
                                 batchAspects.contains(aspectName);
          return !existsInGMS && !existsInBatch;
        })
        .collect(Collectors.toSet());

    if (!missingAspects.isEmpty()) {
      String message = String.format(
          "Dataset %s is missing required governance aspects: %s. " +
          "All datasets must have ownership, tags, and domain set.",
          datasetUrn,
          String.join(", ", missingAspects)
      );

      logger.warn("Validation failed: {}", message);

      // Find a BatchItem for this dataset to include in the exception
      BatchItem relevantItem = mcpItems.stream()
          .filter(item -> datasetUrn.equals(item.getUrn()))
          .findFirst()
          .orElse(null);

      return AspectValidationException.forItem(relevantItem, message);
    }

    return null;
  }

  @Nonnull
  @Override
  public AspectPluginConfig getConfig() {
    return config;
  }

  @Override
  public DatasetGovernanceValidator setConfig(@Nonnull AspectPluginConfig config) {
    this.config = config;
    return this;
  }
}
```

## Step 4: Configure the Model Plugin

Create the directory:

```bash
mkdir -p src/main/resources
```

Create `src/main/resources/entity-registry.yml`:

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

## Step 5: Build and Deploy the Validator

```bash
# Build the validator JAR
./gradlew build

# Deploy as a DataHub model plugin
./gradlew deployPlugin
```

This creates the plugin structure at:

```
~/.datahub/plugins/models/
└── dataset-governance-validator/
    └── 1.0.0/
        ├── datahub-dataset-governance-validator-1.0.0.jar
        └── entity-registry.yml
```

## Step 6: Load the Plugin in DataHub

For local development with `quickstartDebug`, DataHub automatically discovers plugins from `~/.datahub/plugins/models`.

For production deployments, mount the plugin directory into your DataHub GMS container:

```yaml
# docker-compose.override.yml
services:
  datahub-gms:
    volumes:
      - ~/.datahub/plugins:/etc/datahub/plugins/models
```

## Step 7: Restart DataHub GMS

```bash
# For local development
docker restart datahub-datahub-gms-debug-1

# Monitor logs to confirm plugin loaded
docker logs -f datahub-datahub-gms-debug-1 | grep -i "validator"
```

You should see:

```
Enabled 1 plugins. [com.linkedin.metadata.aspect.plugins.validation.DatasetGovernanceValidator]
```

## Testing the Validator

### Method 1: Using DataHub Python Client

Create `test_batch_validator.py`:

```python
#!/usr/bin/env python3

from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    DomainsClass
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp

# Configure emitter
emitter = DataHubRestEmitter(gms_server="http://localhost:8080")

# Test 1: Invalid dataset (missing governance) - should fail
print("Testing invalid dataset...")
dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.test_dataset,PROD)"

try:
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            name="test_dataset",
            description="Dataset without governance - should be blocked"
        )
    )
    emitter.emit_mcps([mcp])
    print("❌ ERROR: Expected validation failure!")
except Exception as e:
    print(f"✅ SUCCESS: Validator blocked dataset: {str(e)}")

# Test 2: Valid dataset (has all governance) - should succeed
print("\nTesting valid dataset...")
audit_stamp = AuditStamp(time=1640995200000, actor="urn:li:corpuser:datahub")

mcps = [
    MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(name="test_dataset")
    ),
    MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=OwnershipClass(
            owners=[OwnerClass(
                owner="urn:li:corpuser:datahub",
                type=OwnershipTypeClass.DATAOWNER
            )],
            lastModified=audit_stamp
        )
    ),
    MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(tag="urn:li:tag:Production")]
        )
    ),
    MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DomainsClass(domains=["urn:li:domain:engineering"])
    )
]

try:
    emitter.emit_mcps(mcps)
    print("✅ SUCCESS: Valid dataset accepted!")
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
```

Run the test:

```bash
# Using DataHub's metadata-ingestion venv
cd /path/to/datahub/metadata-ingestion
source venv/bin/activate
python test_batch_validator.py
```

### Method 2: Using REST API

Test with curl (individual proposal - should fail):

```bash
curl -X POST "http://localhost:8080/entities?action=ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "entity": {
      "value": {
        "com.linkedin.metadata.snapshot.DatasetSnapshot": {
          "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.test_api,PROD)",
          "aspects": [{
            "com.linkedin.dataset.DatasetProperties": {
              "name": "test_api",
              "description": "Test dataset"
            }
          }]
        }
      }
    }
  }'
```

### Expected Results

1. **Invalid Dataset Test**: Returns HTTP 422 with message:

   ```
   Dataset urn:li:dataset:(urn:li:dataPlatform:mysql,db.test_dataset,PROD) is missing
   required governance aspects: ownership, domains, globalTags. All datasets must have
   ownership, tags, and domain set.
   ```

2. **Valid Dataset Test**: Successfully ingests the dataset with all governance metadata.

## How It Works

### Validation Flow

1. **Batch Reception**: The validator receives all MCPs in a batch through `validateProposedAspects`
2. **Dataset Filtering**: Filters MCPs to only process dataset entity types
3. **Dual Checking**: For each dataset URN, checks if required aspects exist in:
   - **GMS**: Already stored aspects (via `AspectRetriever.getLatestSystemAspects`)
   - **Current Batch**: Aspects being set in this batch of changes
4. **Validation Decision**: If any required aspect is missing from both sources, validation fails
5. **Error Reporting**: Returns specific error messages indicating which aspects are missing

### Key Design Decisions

- **Batch-Level Validation**: Validates entire batches to handle related changes together
- **Efficient Fetching**: Uses batch fetching from GMS to minimize database calls
- **Clear Errors**: Provides actionable error messages listing missing aspects
- **Dataset-Only**: Only validates datasets, allowing other entities to pass through

## Production Considerations

### Performance

- Uses batch fetching to minimize GMS queries
- Only processes dataset entities
- Caches nothing - always checks current state

### Extensibility

- Easy to add more required aspects
- Can be extended to other entity types
- Configuration could be externalized

### Compatibility

- Works with DataHub 0.13.2+
- No core DataHub modifications required
- Deployable as external JAR

## Troubleshooting

### Validator Not Loading

Check GMS logs for plugin loading:

```bash
docker logs datahub-datahub-gms-debug-1 | grep -i "plugin"
```

### Validation Not Triggering

1. Verify entity-registry.yml has correct `supportedOperations`
2. Check that the JAR is in the correct plugin directory
3. Ensure GMS was restarted after deploying the plugin

### Authentication Issues

For testing, you may need to disable authentication:

```yaml
# application.yaml
authentication:
  enabled: false
```

## Next Steps

- **Configuration**: Add external configuration for required aspects
- **Exemptions**: Support exempting specific datasets via tags
- **Metrics**: Add monitoring for validation failures
- **Expansion**: Create similar validators for other entity types
