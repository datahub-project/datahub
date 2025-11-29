# Entities Overview

SDK V2 provides high-level entity classes that represent DataHub metadata entities. This guide covers common patterns across all entity types.

## Supported Entities

SDK V2 supports all major DataHub entity types including Dataset, Chart, Dashboard, Container, DataFlow, DataJob, MLModel, and MLModelGroup. Each entity type has a dedicated guide with comprehensive examples and API documentation.

See the **Entity Guides** section in the sidebar for the complete list of available entities and their documentation.

## Common Patterns

### Entity Lifecycle

All entities follow a consistent lifecycle:

1. **Construction** - Build entity using fluent builder
2. **Metadata Addition** - Add tags, owners, properties via methods
3. **Persistence** - Upsert or update to DataHub
4. **Loading** - Fetch existing entity from server

```java
// 1. Construction
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

// 2. Metadata Addition
dataset.addTag("pii")
       .addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER);

// 3. Persistence
client.entities().upsert(dataset);

// 4. Loading
Dataset loaded = client.entities().get(datasetUrn);
```

### URN Management

Each entity type has a strongly-typed URN class:

```java
// Dataset
DatasetUrn datasetUrn = dataset.getDatasetUrn();

// Chart
ChartUrn chartUrn = chart.getChartUrn();

// Dashboard
DashboardUrn dashboardUrn = dashboard.getDashboardUrn();

// DataJob
DataJobUrn dataJobUrn = dataJob.getDataJobUrn();

// Generic access
Urn genericUrn = entity.getUrn();
```

**URN Construction:**

```java
// Built automatically from builder
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("db.schema.table")
    .env("PROD")
    .build();
// URN: urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)
```

### Fluent Method Chaining

All mutation methods return `this` for method chaining:

```java
dataset.addTag("pii")
       .addTag("analytics")
       .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
       .addCustomProperty("team", "data-eng")
       .setDomain("urn:li:domain:Finance");
```

### Three Creation Modes

#### Mode 1: From Scratch (Builder)

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .description("My description")
    .build();
// aspectCache populated with builder aspects
// pendingPatches empty
```

#### Mode 2: Loaded from Server

```java
DatasetUrn urn = new DatasetUrn("snowflake", "my_table", "PROD");
Dataset dataset = client.entities().get(urn);
// aspectCache populated with server aspects
// Aspects have timestamps for freshness tracking
```

#### Mode 3: Reference with Lazy Loading

```java
// Entity bound to client enables lazy loading
Dataset dataset = client.entities().reference(urn);
dataset.bindToClient(client, mode);
String desc = dataset.getDescription();  // Fetches on first access
```

## Aspect Management

### Aspect Caching

Entities cache aspects locally with TTL-based freshness:

```java
// Cached aspects with 60-second default TTL
dataset.setCacheTtlMs(120000);  // 2 minutes

// Check cache status
Map<String, RecordTemplate> aspects = dataset.getAllAspects();
```

### Lazy Loading

When bound to a client, aspects are fetched on-demand:

```java
Dataset dataset = client.entities().get(urn);
// aspectCache may not have all aspects

String description = dataset.getDescription();
// Triggers lazy fetch if not cached or expired
```

## Patch-Based Operations

### Pending Patches

Mutations accumulate as patches until save:

```java
dataset.addTag("tag1");     // Creates patch
dataset.addTag("tag2");     // Creates patch
dataset.addOwner("user", OwnershipType.TECHNICAL_OWNER);  // Creates patch

// Check pending patches
boolean hasPending = dataset.hasPendingPatches();
List<MetadataChangeProposal> patches = dataset.getPendingPatches();

// Emit all patches
client.entities().update(dataset);

// Patches cleared after emission
dataset.clearPendingPatches();
```

### Understanding What Gets Sent

The `upsert()` method emits everything accumulated on the entity:

1. **Cached aspects** (from builder)
2. **Full aspect replacements** (from `set*()` methods)
3. **Patches** (from `add*/remove*` methods)

**What gets sent depends on how the entity was created and what operations you performed:**

**Builder with patches:**

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .description("Description")
    .build();
dataset.addTag("pii");  // Creates patch
client.entities().upsert(dataset);  // Sends: cached aspects + tag patch
```

**Patches only (loaded entity):**

```java
Dataset dataset = client.entities().get(urn);
dataset.addTag("pii");  // Creates patch
client.entities().upsert(dataset);  // Sends: tag patch only
```

**Full aspect replacement:**

```java
Dataset dataset = client.entities().get(urn);
dataset.setDescription("New description");  // Creates full aspect MCP
client.entities().upsert(dataset);  // Sends: complete description aspect
```

**Combined operations:**

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();
dataset.setDescription("Description");  // Full aspect MCP
dataset.addTag("pii");  // Patch
dataset.addOwner("user", OwnershipType.TECHNICAL_OWNER);  // Patch
client.entities().upsert(dataset);  // Sends: cached aspects + description aspect + 2 patches
```

## Mode-Aware Operations

Entities respect the client's operation mode:

```java
// SDK mode client
DataHubClientV2 sdkClient = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(OperationMode.SDK)  // Default
    .build();

dataset.setDescription("User description");
sdkClient.entities().upsert(dataset);
// Writes to editableDatasetProperties

// INGESTION mode client
DataHubClientV2 ingestionClient = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(OperationMode.INGESTION)
    .build();

dataset.setDescription("Ingested description");
ingestionClient.entities().upsert(dataset);
// Writes to datasetProperties
```

## Common Metadata Operations

### Tags

```java
// Add tags
entity.addTag("pii");
entity.addTag("urn:li:tag:analytics");

// Remove tags
entity.removeTag("pii");
```

### Owners

```java
import com.linkedin.common.OwnershipType;

// Add owners
entity.addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER);
entity.addOwner("urn:li:corpuser:jane", OwnershipType.DATA_STEWARD);

// Remove owners
entity.removeOwner("urn:li:corpuser:john");
```

### Glossary Terms

```java
// Add terms
entity.addTerm("urn:li:glossaryTerm:CustomerData");
entity.addTerm("urn:li:glossaryTerm:PII");

// Remove terms
entity.removeTerm("urn:li:glossaryTerm:CustomerData");
```

### Domain

```java
// Set domain
entity.setDomain("urn:li:domain:Marketing");

// Remove domain
entity.removeDomain();
```

### Custom Properties

```java
// Add custom properties
entity.addCustomProperty("team", "data-engineering");
entity.addCustomProperty("retention", "90_days");

// Remove custom properties
entity.removeCustomProperty("retention");
```

## Error Handling

Handle exceptions gracefully:

```java
try {
    client.entities().upsert(dataset);
} catch (IOException e) {
    // Network or serialization errors
    log.error("I/O error: {}", e.getMessage());
} catch (ExecutionException e) {
    // Server-side errors
    log.error("Server error: {}", e.getCause().getMessage());
} catch (InterruptedException e) {
    // Operation cancelled
    Thread.currentThread().interrupt();
    log.error("Operation interrupted");
}
```

## Entity-Specific Guides

Each entity type has a dedicated guide with detailed information including:

- URN structure and construction
- Entity-specific properties and operations
- Comprehensive code examples
- Best practices and common patterns
- Lineage and relationship management

**See the Entity Guides section in the sidebar** for the complete list of available entity documentation.

Example guides include [Dataset Entity](./dataset-entity.md), [Chart Entity](./chart-entity.md), [Dashboard Entity](./dashboard-entity.md), and [DataJob Entity](./datajob-entity.md).
