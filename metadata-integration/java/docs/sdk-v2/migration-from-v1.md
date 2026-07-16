# Migration Guide: V1 to V2

This guide helps you migrate from Java SDK V1 (RestEmitter) to V2 (DataHubClientV2). We'll show side-by-side examples and highlight key differences.

## Why Migrate?

V2 offers significant improvements over V1:

- ✅ **Type-safe entity builders** instead of manual MCP construction
- ✅ **Automatic URN generation** instead of string manipulation
- ✅ **Patch-based updates** for efficient incremental changes
- ✅ **Fluent API** with method chaining
- ✅ **Lazy loading** with caching
- ✅ **Mode-aware operations** (SDK vs INGESTION)

## Key Differences

| Aspect               | V1 (RestEmitter)        | V2 (DataHubClientV2)         |
| -------------------- | ----------------------- | ---------------------------- |
| **Abstraction**      | Low-level MCPs          | High-level entities          |
| **URN Construction** | Manual strings          | Automatic from builder       |
| **Updates**          | Full aspect replacement | Patch-based incremental      |
| **Type Safety**      | Minimal                 | Strong compile-time checking |
| **API Style**        | Imperative emission     | Fluent builders              |
| **Entity Support**   | Generic MCPs            | Dataset, Chart, Dashboard    |

## Migration Examples

### Example 1: Creating a Dataset

**V1 (RestEmitter):**

```java
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.common.urn.DatasetUrn;

// Manual URN construction
DatasetUrn urn = new DatasetUrn(
    new DataPlatformUrn("snowflake"),
    "my_database.my_schema.my_table",
    FabricType.PROD
);

// Manual aspect construction
DatasetProperties props = new DatasetProperties();
props.setDescription("My dataset description");
props.setName("My Dataset");

// Manual MCP construction
MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.builder()
    .entityType("dataset")
    .entityUrn(urn)
    .upsert()
    .aspect(props)
    .build();

// Create emitter
RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080"));

// Emit
emitter.emit(mcp, null).get();
```

**V2 (DataHubClientV2):**

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;

// Fluent builder
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_database.my_schema.my_table")
    .env("PROD")
    .description("My dataset description")
    .displayName("My Dataset")
    .build();

// Create client
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .build();

// Upsert (URN auto-generated, aspect auto-wired)
client.entities().upsert(dataset);
```

**Changes:**

- ❌ No manual URN construction
- ❌ No manual aspect creation
- ❌ No MCP wrapper construction
- ✅ Fluent builder handles everything
- ✅ Type-safe method calls
- ✅ Automatic aspect wiring

### Example 2: Adding Tags

**V1 (RestEmitter):**

```java
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;

// Fetch existing tags or create new
GlobalTags tags = fetchExistingTags(urn);  // You implement this
if (tags == null) {
    tags = new GlobalTags();
    tags.setTags(new TagAssociationArray());
}

// Add new tag
TagAssociation newTag = new TagAssociation();
newTag.setTag(new TagUrn("pii"));
tags.getTags().add(newTag);

// Create MCP to replace entire GlobalTags aspect
MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.builder()
    .entityType("dataset")
    .entityUrn(urn)
    .upsert()
    .aspect(tags)
    .build();

emitter.emit(mcp, null).get();
```

**V2 (DataHubClientV2):**

```java
// Just add the tag - patch handles everything
dataset.addTag("pii");
client.entities().update(dataset);
```

**Changes:**

- ❌ No fetching existing tags
- ❌ No manual aspect manipulation
- ❌ No MCP construction
- ✅ Single method call
- ✅ Patch-based (doesn't overwrite other tags)
- ✅ Automatic URN handling

### Example 3: Adding Owners

**V1 (RestEmitter):**

```java
import com.linkedin.common.Ownership;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;

// Fetch existing owners or create new
Ownership ownership = fetchExistingOwnership(urn);
if (ownership == null) {
    ownership = new Ownership();
    ownership.setOwners(new OwnerArray());
}

// Add new owner
Owner newOwner = new Owner();
newOwner.setOwner(Urn.createFromString("urn:li:corpuser:john_doe"));
newOwner.setType(OwnershipType.TECHNICAL_OWNER);
ownership.getOwners().add(newOwner);

// Create MCP
MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.builder()
    .entityType("dataset")
    .entityUrn(urn)
    .upsert()
    .aspect(ownership)
    .build();

emitter.emit(mcp, null).get();
```

**V2 (DataHubClientV2):**

```java
dataset.addOwner("urn:li:corpuser:john_doe", OwnershipType.TECHNICAL_OWNER);
client.entities().update(dataset);
```

**Changes:**

- ❌ No fetching existing owners
- ❌ No manual Owner object creation
- ❌ No array manipulation
- ✅ Single method with parameters
- ✅ Type-safe ownership type enum
- ✅ Automatic patch creation

### Example 4: Multiple Metadata Additions

**V1 (RestEmitter):**

```java
// Create dataset properties
DatasetProperties props = new DatasetProperties();
props.setDescription("My description");

// Create tags
GlobalTags tags = new GlobalTags();
TagAssociationArray tagArray = new TagAssociationArray();
tagArray.add(createTagAssociation("pii"));
tagArray.add(createTagAssociation("sensitive"));
tags.setTags(tagArray);

// Create ownership
Ownership ownership = new Ownership();
OwnerArray ownerArray = new OwnerArray();
ownerArray.add(createOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER));
ownership.setOwners(ownerArray);

// Create 3 separate MCPs and emit each
emitter.emit(createMCP(urn, props), null).get();
emitter.emit(createMCP(urn, tags), null).get();
emitter.emit(createMCP(urn, ownership), null).get();
```

**V2 (DataHubClientV2):**

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .description("My description")
    .build();

dataset.addTag("pii")
       .addTag("sensitive")
       .addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER);

client.entities().upsert(dataset);  // Single call, all metadata included
```

**Changes:**

- ❌ No creating multiple aspects separately
- ❌ No multiple emission calls
- ✅ Method chaining for fluent API
- ✅ Single upsert emits everything
- ✅ Atomic operation

### Example 5: Updating Existing Entity

**V1 (RestEmitter):**

```java
// 1. Fetch current state from DataHub
DatasetProperties existingProps = fetchAspect(urn, DatasetProperties.class);

// 2. Modify
existingProps.setDescription("Updated description");

// 3. Send back (overwrites entire aspect)
MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.builder()
    .entityType("dataset")
    .entityUrn(urn)
    .upsert()
    .aspect(existingProps)
    .build();

emitter.emit(mcp, null).get();
```

**V2 (DataHubClientV2):**

```java
// Just modify - patch handles incremental update
Dataset dataset = client.entities().get(urn);  // Optional: load existing
dataset.setDescription("Updated description");
client.entities().update(dataset);  // Patch only changes description
```

**Changes:**

- ✅ Can update without fetching (for patches)
- ✅ Patch-based incremental update
- ✅ No risk of overwriting other fields
- ✅ More efficient payload

## Migration Checklist

### 1. Update Dependencies

Keep existing dependency (backwards compatible):

```gradle
dependencies {
    implementation 'io.acryl:datahub-client:__version__'
}
```

### 2. Change Imports

**Replace:**

```java
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
```

**With:**

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import datahub.client.v2.entity.Chart;
```

### 3. Replace RestEmitter with DataHubClientV2

**Before:**

```java
RestEmitter emitter = RestEmitter.create(b -> b
    .server("http://localhost:8080")
    .token("my-token")
);
```

**After:**

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .token("my-token")
    .build();
```

### 4. Use Entity Builders

Replace manual MCP/URN construction with entity builders:

**Before:**

```java
DatasetUrn urn = new DatasetUrn(...);
DatasetProperties props = new DatasetProperties();
props.setDescription("...");
MetadataChangeProposalWrapper mcp = MetadataChangeProposalWrapper.builder()...
emitter.emit(mcp, null).get();
```

**After:**

```java
Dataset dataset = Dataset.builder()
    .platform("...")
    .name("...")
    .description("...")
    .build();
client.entities().upsert(dataset);
```

### 5. Use Patch Operations for Updates

Replace fetch-modify-send with patches:

**Before:**

```java
GlobalTags tags = fetch(...);
tags.getTags().add(...);
emit(tags);
```

**After:**

```java
dataset.addTag("...");
client.entities().update(dataset);
```

## Gradual Migration Strategy

You can migrate incrementally - V1 and V2 can coexist:

```java
// V1 emitter (for unsupported operations)
RestEmitter emitter = RestEmitter.create(b -> b.server("..."));

// V2 client (for entities)
DataHubClientV2 client = DataHubClientV2.builder()
    .server("...")
    .build();

// Use V2 for supported entities
Dataset dataset = Dataset.builder()...
client.entities().upsert(dataset);

// Fall back to V1 for custom MCPs
MetadataChangeProposalWrapper customMcp = ...;
emitter.emit(customMcp, null).get();
```

## Common Pitfalls

### 1. Forgetting to Call `update()` or `upsert()`

**Problem:**

```java
dataset.addTag("pii");  // Patch created but not emitted!
// Missing: client.entities().update(dataset);
```

**Solution:**
Always call `update()` or `upsert()` to emit changes.

### 2. Using V1 Pattern with V2 Entities

**Problem:**

```java
Dataset dataset = Dataset.builder()...;
// Don't do this - use client.entities() instead
emitter.emit(dataset.toMCPs(), null);  // Wrong!
```

**Solution:**
Use V2's EntityClient:

```java
client.entities().upsert(dataset);
```

### 3. Mixing Operation Modes

**Problem:**

```java
// Client in SDK mode
DataHubClientV2 client = DataHubClientV2.builder()
    .operationMode(OperationMode.SDK)
    .build();

// But manually setting system description (conflicts with mode)
dataset.setSystemDescription("...");  // Inconsistent!
```

**Solution:**
Use mode-aware methods or match mode to explicit methods:

```java
dataset.setDescription("...");  // Mode-aware
```

## Benefits After Migration

- **50-80% less code** for common operations
- **Type safety** catches errors at compile time
- **Better performance** with patches
- **Easier testing** with mock entities
- **Better IDE support** with autocomplete

## Need Help?

- **V2 Documentation**: [Getting Started Guide](./getting-started.md)
- **Entity Guides**: [Dataset](./dataset-entity.md), [Chart](./chart-entity.md)
- **Examples**: [V2 Examples Directory](../../examples/src/main/java/io/datahubproject/examples/v2/)
- **V1 Documentation**: [Java SDK V1](../../as-a-library.md) (for reference)

## Still Using V1 Features?

Some advanced features are still V1-only:

- **KafkaEmitter** - Use V1 for Kafka-based emission
- **FileEmitter** - Use V1 for file-based emission
- **Custom MCPs** - Use V1 for entity types not yet in V2
- **Direct aspect access** - Use V1 for fine-grained control

You can use both V1 and V2 in the same application!
