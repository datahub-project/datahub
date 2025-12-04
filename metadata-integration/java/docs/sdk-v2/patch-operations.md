# Patch Operations Guide

SDK V2 uses **patch-based updates** for efficient, surgical modifications to metadata. This guide explains how patches work and when to use them.

## What Are Patches?

Patches are **incremental updates** that modify specific fields without replacing entire aspects. Instead of sending the full `datasetProperties` aspect, a patch sends only the changes.

### Patch vs Full Update

**Full Update (V1 Style):**

```java
// Fetch entire aspect
DatasetProperties props = getDatasetProperties(urn);

// Modify one field
props.setDescription("New description");

// Send entire aspect back (overwrites everything)
sendAspect(urn, props);
```

**Patch Update (V2 Style):**

```java
// Send only the change
dataset.setDescription("New description");
client.entities().update(dataset);
// Sends JSON Patch: { "op": "add", "path": "/description", "value": "New description" }
```

### Benefits of Patches

1. **Efficiency** - Only changed fields sent over network
2. **Concurrency Safety** - Less risk of overwriting concurrent changes
3. **Atomicity** - Multiple patches applied together or not at all
4. **Bandwidth** - Reduced payload size

## How Patches Work in SDK V2

### Patch Accumulation Pattern

Entities accumulate patches in a pending list until save:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

// Each method creates a patch MCP and adds to pendingPatches list
dataset.addTag("pii");              // Patch 1
dataset.addTag("sensitive");        // Patch 2
dataset.addOwner("user", OwnershipType.TECHNICAL_OWNER);  // Patch 3

// Check pending patches
System.out.println("Pending patches: " + dataset.getPendingPatches().size());
// Output: Pending patches: 3

// Emit all patches atomically
client.entities().update(dataset);

// Patches cleared after emission
System.out.println("Pending patches: " + dataset.getPendingPatches().size());
// Output: Pending patches: 0
```

### Under the Hood

```java
// From Dataset.java
public Dataset addTag(@Nonnull String tagUrn) {
    // Create patch using existing patch builder
    GlobalTagsPatchBuilder patch = new GlobalTagsPatchBuilder()
        .urn(getUrn())
        .addTag(tag, null);

    // Add to pending patches list
    addPatchMcp(patch.build());

    return this;
}
```

When `update()` is called:

```java
// From EntityClient.java
public void upsert(Entity entity) {
    if (entity.hasPendingPatches()) {
        // Emit patches
        for (MetadataChangeProposal patchMcp : entity.getPendingPatches()) {
            emitter.emit(patchMcp, null);
        }
        entity.clearPendingPatches();
    } else {
        // No patches, emit full aspects
        for (MetadataChangeProposalWrapper mcp : entity.toMCPs()) {
            emitter.emit(mcp);
        }
    }
}
```

## Reusing Existing Patch Builders

SDK V2 **reuses existing patch builders** from `datahub.client.patch` package:

### Available Patch Builders

| Builder                                 | Purpose                    | Example                                   |
| --------------------------------------- | -------------------------- | ----------------------------------------- |
| `OwnershipPatchBuilder`                 | Add/remove owners          | `addOwner()`, `removeOwner()`             |
| `GlobalTagsPatchBuilder`                | Add/remove tags            | `addTag()`, `removeTag()`                 |
| `GlossaryTermsPatchBuilder`             | Add/remove terms           | `addTerm()`, `removeTerm()`               |
| `DomainsPatchBuilder`                   | Set/remove domain          | `setDomain()`, `removeDomain()`           |
| `DatasetPropertiesPatchBuilder`         | Update properties          | `setDescription()`, `addCustomProperty()` |
| `EditableDatasetPropertiesPatchBuilder` | Update editable properties | `setEditableDescription()`                |

### Why Reuse?

- **Battle-tested** - Used by Python SDK V2 in production
- **Correctness** - Complex JSON Patch logic already validated
- **Consistency** - Same semantics across language SDKs
- **Maintainability** - Single implementation to maintain

## When to Use Patches

### Use Patches For:

✅ **Incremental changes to existing entities**

```java
Dataset dataset = client.entities().get(urn);
dataset.addTag("new-tag");
client.entities().update(dataset);  // Patch
```

✅ **Adding metadata to entities**

```java
dataset.addOwner("urn:li:corpuser:new_owner", OwnershipType.TECHNICAL_OWNER);
dataset.addCustomProperty("updated_at", String.valueOf(System.currentTimeMillis()));
client.entities().update(dataset);  // Multiple patches
```

✅ **Surgical updates without full entity knowledge**

```java
// Don't need to fetch entire entity
dataset.addTag("gdpr");
client.entities().update(dataset);  // Just adds tag
```

### Use Full Upsert For:

✅ **Creating new entities**

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .description("New dataset")
    .build();

client.entities().upsert(dataset);  // Full upsert
```

✅ **Replacing entire aspects**

```java
// Set complete schema
SchemaMetadata schema = buildCompleteSchema();
dataset.setSchema(schema);
client.entities().upsert(dataset);  // Sends full schema aspect
```

✅ **Builder-provided metadata**

```java
Dataset dataset = Dataset.builder()
    .platform("postgres")
    .name("my_table")
    .description("Description from builder")
    .build();

// Builder populates aspectCache with full aspects
client.entities().upsert(dataset);  // Sends cached aspects
```

## Patch Operations by Entity

### Dataset Patches

**Ownership:**

```java
dataset.addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER);
dataset.removeOwner("urn:li:corpuser:jane");
```

**Tags:**

```java
dataset.addTag("pii");
dataset.removeTag("deprecated");
```

**Glossary Terms:**

```java
dataset.addTerm("urn:li:glossaryTerm:CustomerData");
dataset.removeTerm("urn:li:glossaryTerm:OldTerm");
```

**Domain:**

```java
dataset.setDomain("urn:li:domain:Marketing");
dataset.removeDomain();
```

**Properties:**

```java
dataset.addCustomProperty("team", "data-eng");
dataset.removeCustomProperty("old_property");
dataset.setDescription("New description");
```

### Chart Patches

Chart supports the same patch operations as Dataset:

```java
chart.addOwner("urn:li:corpuser:analyst", OwnershipType.TECHNICAL_OWNER);
chart.addTag("visualization");
chart.addTerm("urn:li:glossaryTerm:SalesMetrics");
chart.setDomain("urn:li:domain:BusinessIntelligence");
```

See [Chart Entity Guide](./chart-entity.md) for complete details.

## Advanced: Manual Patch Construction

For advanced use cases, construct patches directly:

```java
import com.linkedin.metadata.aspect.patch.builder.OwnershipPatchBuilder;
import com.linkedin.common.urn.Urn;

// Manual patch construction
OwnershipPatchBuilder patchBuilder = new OwnershipPatchBuilder()
    .urn(dataset.getUrn())
    .addOwner(
        Urn.createFromString("urn:li:corpuser:alice"),
        OwnershipType.DATA_STEWARD
    );

MetadataChangeProposal patch = patchBuilder.build();

// Add to entity's pending patches
dataset.addPatchMcp(patch);

// Or emit directly
emitter.emit(patch, null);
```

## Patch vs Upsert Decision Tree

```
New entity from builder?
├─ Yes → Use upsert() (sends cached aspects)
└─ No → Loaded from server or reference?
    ├─ Yes → Making incremental changes?
    │   ├─ Yes → Use update() (sends patches)
    │   └─ No → Replacing entire aspect?
    │       └─ Yes → Use upsert() (sends full aspect)
    └─ No → Just adding tags/owners/etc?
        └─ Yes → Use update() (sends patches)
```

## Pending Patches Management

### Check for Pending Patches

```java
if (dataset.hasPendingPatches()) {
    System.out.println("Entity has pending patches");
}
```

### Get Pending Patches

```java
List<MetadataChangeProposal> patches = dataset.getPendingPatches();
for (MetadataChangeProposal patch : patches) {
    System.out.println("Patch for aspect: " + patch.getAspectName());
}
```

### Clear Pending Patches

```java
// Manually clear without emitting
dataset.clearPendingPatches();
```

### Batch Multiple Changes

```java
// Accumulate many patches
dataset.addTag("tag1")
       .addTag("tag2")
       .addTag("tag3")
       .addOwner("user1", OwnershipType.TECHNICAL_OWNER)
       .addOwner("user2", OwnershipType.DATA_STEWARD)
       .addCustomProperty("key1", "value1")
       .addCustomProperty("key2", "value2");

// All 7 patches emitted in single update() call
client.entities().update(dataset);
```

## Performance Considerations

### Network Efficiency

```java
// Inefficient: 3 separate network calls
dataset.addTag("tag1");
client.entities().update(dataset);
dataset.addTag("tag2");
client.entities().update(dataset);
dataset.addTag("tag3");
client.entities().update(dataset);

// Efficient: 1 network call with 3 patches
dataset.addTag("tag1")
       .addTag("tag2")
       .addTag("tag3");
client.entities().update(dataset);
```

### Payload Size

**Full upsert (datasetProperties):**

- ~2-5 KB for typical dataset aspect

**Patch (add tag):**

- ~200-300 bytes for single tag patch

**10 tags:** Patches = ~3 KB, Full upsert = ~5 KB

## JSON Patch Format

Patches use [JSON Patch (RFC 6902)](https://datatracker.ietf.org/doc/html/rfc6902) format:

**Add operation:**

```json
{
  "op": "add",
  "path": "/tags/urn:li:tag:pii",
  "value": {
    "tag": "urn:li:tag:pii"
  }
}
```

**Remove operation:**

```json
{
  "op": "remove",
  "path": "/tags/urn:li:tag:deprecated"
}
```

SDK V2 abstracts this complexity - you work with Java methods, not JSON.

## Troubleshooting

### Patches Not Applied

**Issue:** Changes not visible in DataHub

**Solutions:**

- Verify `update()` was called (patches don't emit automatically)
- Check for errors in emission response
- Ensure entity is bound to client

### Concurrent Updates

**Issue:** Patches conflict with concurrent changes

**Solutions:**

- Patches are generally safe for concurrent updates
- Each patch is atomic
- For complex scenarios, load entity first to get latest state

### Patch Cleared Unexpectedly

**Issue:** Pending patches disappear

**Reason:** `upsert()` or `update()` clears patches after emission

**Solution:** This is expected behavior - patches are one-time use

## Next Steps

- **[Design Principles](./design-principles.md)** - Architecture behind patches
- **[Dataset Entity Guide](./dataset-entity.md)** - All patch operations for datasets
- **[Migration Guide](./migration-from-v1.md)** - Moving from full updates to patches

## API Reference

Key classes:

- Entity.java - Patch accumulation
- EntityClient.java - Patch emission
- datahub.client.patch.\* - Patch builders
