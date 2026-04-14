# Design Principles of Java SDK V2

This document provides an architectural overview of DataHub Java SDK V2, exploring the engineering principles and design patterns that enable its type-safe, efficient metadata management capabilities.

## Architectural Philosophy

SDK V2 is built on a foundation of **pragmatic reuse, intelligent caching, and layered abstractions**. Rather than reinventing infrastructure, it composes proven components into a coherent, intuitive API while introducing new patterns for efficient metadata operations.

### Core Tenets

1. **Leverage Existing Infrastructure** - Build atop battle-tested components
2. **Type Safety as a First-Class Concern** - Exploit Java's type system for compile-time correctness
3. **Separation of Concerns** - Clear boundaries between entity, operations, and transport layers
4. **Efficiency Through Patches** - Surgical updates over full replacements
5. **Intelligent Resource Management** - Lazy loading, caching, and batching

## Layer Architecture

SDK V2 employs a three-layer architecture with clear separation of responsibilities:

```
┌─────────────────────────────────────────────────────────────┐
│                    Entity Layer                              │
│  (Dataset, Chart, Dashboard - Business Logic)                │
│  - Fluent builders for entity construction                   │
│  - Patch accumulation and aspect management                  │
│  - Mode-aware behavior (SDK vs INGESTION)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                 Operations Layer                             │
│  (EntityClient - CRUD Operations)                            │
│  - Entity lifecycle management                               │
│  - Patch vs full aspect emission logic                       │
│  - Lazy loading coordination                                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                  Transport Layer                             │
│  (RestEmitter, Patch Builders)                               │
│  - HTTP communication with DataHub                           │
│  - MCP serialization and emission                            │
│  - Patch builder integration                                 │
└─────────────────────────────────────────────────────────────┘
```

## Design Patterns

### 1. Fluent Builder Pattern

Entity construction follows a **fluent builder pattern** that guides developers through required fields and provides IDE autocomplete support:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("analytics.public.events")
    .env("PROD")
    .description("User events")
    .build();
```

**Engineering Benefits:**

- **Compile-time validation** - Missing required fields (platform, name) fail at compilation
- **Immutable construction** - Builder accumulates state; `build()` creates immutable entity
- **Discoverability** - IDE autocomplete reveals available methods
- **Extensibility** - New optional parameters added without breaking existing code

### 2. Patch Accumulation Pattern

Rather than modifying aspects directly, mutations create **patch MCPs** that accumulate in a pending list:

```java
dataset.addTag("pii")                          // Creates patch MCP
       .addOwner("user", TECHNICAL_OWNER)      // Creates patch MCP
       .addCustomProperty("retention", "90");  // Creates patch MCP

client.entities().upsert(dataset);  // Emits all patches atomically
```

**Engineering Benefits:**

- **Deferred execution** - Batches multiple changes into a single network round-trip
- **Atomic updates** - All patches applied together or none
- **Efficient transmission** - Only changed fields sent over wire
- **Reuse of proven infrastructure** - Leverages existing `datahub.client.patch` builders

**Implementation Detail:**
Entity base class maintains multiple change tracking mechanisms:

```java
// From Entity.java
protected final Map<String, RecordTemplate> aspectCache;        // Cached aspects from builder
protected final List<MetadataChangeProposalWrapper> pendingMCPs; // Full aspect replacements
protected final List<MetadataChangeProposal> pendingPatches;     // Incremental patches
```

Each mutation (addTag, addOwner) creates a patch using existing builders:

```java
// From Dataset.java
public Dataset addTag(@Nonnull String tagUrn) {
    GlobalTagsPatchBuilder patch = new GlobalTagsPatchBuilder()
        .urn(getUrn())
        .addTag(tag, null);
    addPatchMcp(patch.build());  // Adds to pendingPatches list
    return this;
}
```

When `EntityClient.upsert()` is called, it emits **everything** accumulated on the entity in order:

```java
// From EntityClient.upsert()

// Step 1: Emit cached aspects (from builder)
if (!entity.toMCPs().isEmpty()) {
    for (MetadataChangeProposalWrapper mcp : entity.toMCPs()) {
        emitter.emit(mcp);
    }
}

// Step 2: Emit pending full aspect MCPs (from set*() methods)
if (entity.hasPendingMCPs()) {
    for (MetadataChangeProposalWrapper mcp : entity.getPendingMCPs()) {
        emitter.emit(mcp);
    }
    entity.clearPendingMCPs();
}

// Step 3: Emit all pending patches (from add*/remove* methods)
if (entity.hasPendingPatches()) {
    for (MetadataChangeProposal patchMcp : entity.getPendingPatches()) {
        emitter.emit(patchMcp, null);
    }
    entity.clearPendingPatches();
}
```

**Key insight:** `upsert()` is not an either/or operation - it emits **all** accumulated changes. What gets sent depends on what you've accumulated on the entity, not which method you call.

### 3. Lazy Loading with TTL-Based Caching

Entities support **lazy aspect loading** to minimize network calls while ensuring data freshness:

```java
// Entity maintains aspect cache with timestamps
protected final Map<String, RecordTemplate> aspectCache;
protected final Map<String, Long> aspectTimestamps;
protected long cacheTtlMs = 60000;  // 60-second default TTL
```

**Loading Strategy:**

1. **Cache-only access** (`getAspectCached`) - Returns cached aspect or null
2. **Lazy loading** (`getAspectLazy`) - Checks cache freshness, fetches from server if stale
3. **Get-or-create** (`getOrCreateAspect`) - Returns cached or creates new empty aspect locally

**Implementation:**

```java
protected <T extends RecordTemplate> T getAspectLazy(@Nonnull Class<T> aspectClass) {
    String aspectName = getAspectName(aspectClass);

    // Check cache freshness
    if (aspectCache.containsKey(aspectName)) {
        Long timestamp = aspectTimestamps.get(aspectName);
        if (timestamp != null && System.currentTimeMillis() - timestamp < cacheTtlMs) {
            return aspectClass.cast(aspectCache.get(aspectName));
        }
    }

    // Fetch from server if client is bound
    if (client != null) {
        T aspect = client.getAspect(urn, aspectClass);
        if (aspect != null) {
            aspectCache.put(aspectName, aspect);
            aspectTimestamps.put(aspectName, System.currentTimeMillis());
        }
        return aspect;
    }

    return null;
}
```

**Engineering Benefits:**

- **Network efficiency** - Reduces redundant server calls
- **Freshness guarantee** - Configurable TTL ensures data isn't stale
- **Transparent to caller** - Complexity hidden behind simple getter
- **Client binding** - Entities bound to EntityClient enable lazy loading

### 4. Mode-Aware Aspect Selection

SDK V2 distinguishes between **user-initiated edits** (SDK mode) and **system/pipeline writes** (INGESTION mode):

```java
public enum OperationMode {
    SDK,        // Interactive use - writes to editable aspects
    INGESTION   // ETL pipelines - writes to system aspects
}
```

**Aspect Routing:**

- **SDK Mode** → `editableDatasetProperties`, `editableSchemaMetadata`
- **INGESTION Mode** → `datasetProperties`, `schemaMetadata`

**Implementation:**

```java
public Dataset setDescription(@Nonnull String description) {
    if (isIngestionMode()) {
        return setSystemDescription(description);  // datasetProperties
    } else {
        return setEditableDescription(description); // editableDatasetProperties
    }
}
```

**Engineering Benefits:**

- **Clear provenance** - Distinguishes human vs machine edits
- **UI consistency** - DataHub UI shows editable aspects as user overrides
- **Non-destructive** - System data preserved even when users add documentation
- **Lineage preservation** - Ingestion pipelines can refresh system data without clobbering user edits

### 5. Two Entity Lifecycle Patterns

Entities can be instantiated in two ways, each with distinct semantics:

#### **Pattern 1: Builder Construction (New Entities)**

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();
// aspectCache populated with builder-provided aspects
// aspectTimestamps empty - indicates new entity
```

**Use case:** Creating new entities from scratch

#### **Pattern 2: Server Loading (Existing Entities)**

```java
Dataset dataset = client.entities().get(urn);
// aspectCache populated with server aspects
// aspectTimestamps records fetch time for each aspect
// Entity automatically bound to client for lazy loading
```

**Use case:** Modifying existing entities with current server state. When you access aspects not already cached, the entity will automatically fetch them from the server (lazy loading).

### 6. Client Binding for Lazy Loading

Entities are **automatically bound to an EntityClient** when loaded from the server or during `upsert()` to enable lazy aspect fetching:

```java
public void bindToClient(@Nonnull EntityClient client,
                        @Nonnull OperationMode mode) {
    if (this.client == null) {
        this.client = client;
    }
    if (this.mode == null) {
        this.mode = mode;
    }
}
```

**Binding occurs automatically** during `upsert()`:

```java
// From EntityClient.upsert()
entity.bindToClient(this, config.getMode());
```

**Engineering Benefits:**

- **Transparent lazy loading** - Aspects fetched on first access if not cached
- **Automatic binding** - Entities bound to client during `get()` or `upsert()` operations
- **Mode propagation** - Client mode automatically applied to entity

## Type Safety & Generic Design

### Strongly-Typed Aspect Handling

SDK V2 leverages Java generics to provide compile-time type safety for aspects:

```java
// Type-safe aspect retrieval
protected <T extends RecordTemplate> T getAspectLazy(@Nonnull Class<T> aspectClass) {
    String aspectName = getAspectName(aspectClass);
    RecordTemplate aspect = aspectCache.get(aspectName);
    return aspectClass.cast(aspect);
}

// Usage - compiler enforces type correctness
DatasetProperties props = dataset.getAspectLazy(DatasetProperties.class);
```

**Engineering Benefits:**

- **Compile-time checking** - Type mismatches caught before runtime
- **Refactoring safety** - IDE can trace aspect usages across codebase
- **Autocomplete support** - IDE suggests available aspects
- **Runtime safety** - `ClassCastException` impossible with correct usage

### URN Type Safety

Entity-specific URN types prevent incorrect URN usage:

```java
public class Dataset extends Entity {
    public DatasetUrn getDatasetUrn() {
        return (DatasetUrn) urn;
    }
}

// Compile-time enforcement
DatasetUrn urn = dataset.getDatasetUrn();  // Type-safe
Urn genericUrn = dataset.getUrn();         // Also available
```

## Integration with Existing Infrastructure

### Reuse of Patch Builders

SDK V2 **reuses existing patch builders** from `datahub.client.patch` rather than creating new implementations:

- `OwnershipPatchBuilder` - Owner additions/removals
- `GlobalTagsPatchBuilder` - Tag management
- `GlossaryTermsPatchBuilder` - Term associations
- `DomainsPatchBuilder` - Domain assignment
- `DatasetPropertiesPatchBuilder` - Property updates
- `EditableDatasetPropertiesPatchBuilder` - Editable property updates

**Engineering Benefits:**

- **Battle-tested logic** - Patch builders used in production by Python SDK
- **Consistency** - Same patch semantics across language SDKs
- **Maintainability** - Single implementation to maintain
- **Correctness** - Complex JSON Patch logic already validated

**Example Integration:**

```java
public Dataset addOwner(@Nonnull String ownerUrn, @Nonnull OwnershipType type) {
    Urn owner = Urn.createFromString(ownerUrn);
    OwnershipPatchBuilder patch = new OwnershipPatchBuilder()
        .urn(getUrn())
        .addOwner(owner, type);
    addPatchMcp(patch.build());  // Stores patch MCP
    return this;
}
```

### Leverage RestEmitter

Transport layer reuses `RestEmitter` for HTTP communication:

- Non-blocking emission with futures
- Configurable retries and timeouts
- Token-based authentication
- Async HTTP client pooling

**No changes to RestEmitter** - SDK V2 is purely additive.

## Resource Management & Efficiency

### Batched Emission

Multiple patches accumulated and emitted atomically:

```java
dataset.addTag("tag1").addTag("tag2").addOwner("user1", OWNER);
client.entities().upsert(dataset);  // Single network call, 3 patches
```

### Connection Pooling

RestEmitter uses `CloseableHttpAsyncClient` with connection pooling for efficient HTTP reuse.

### Graceful Degradation

Lazy loading failures logged but don't crash:

```java
catch (Exception e) {
    log.warn("Failed to lazy-load aspect {}: {}", aspectName, e.getMessage());
    return null;  // Graceful degradation
}
```

## Comparison: V1 vs V2 Architecture

| Aspect                | V1 (RestEmitter)               | V2 (DataHubClientV2)        |
| --------------------- | ------------------------------ | --------------------------- |
| **Abstraction Level** | Low - MCPs                     | High - Entities             |
| **URN Construction**  | Manual strings                 | Automatic from builder      |
| **Aspect Wiring**     | Manual MCP building            | Hidden in entity methods    |
| **Updates**           | Full aspect replacement        | Patch-based incremental     |
| **Type Safety**       | Minimal - generic MCPs         | Strong - typed entities     |
| **Lazy Loading**      | Not supported                  | TTL-based caching           |
| **Mode Awareness**    | Not supported                  | SDK vs INGESTION modes      |
| **Learning Curve**    | Steep - requires MCP knowledge | Gentle - intuitive builders |

## Performance Characteristics

### Network Efficiency

- **Patch-based updates**: O(changed_fields) vs O(all_fields)
- **Lazy loading**: Aspects fetched only when accessed
- **Batch emission**: Multiple patches sent in single flush
- **Connection reuse**: HTTP client pooling

### Memory Efficiency

- **Aspect caching**: Only fetched aspects stored
- **TTL expiration**: Stale aspects eligible for GC
- **Lazy instantiation**: Aspects created on-demand

### Time Complexity

- **Entity creation**: O(1) - builder accumulation
- **Patch addition**: O(1) - append to list
- **Upsert operation**: O(n) where n = pending patches or cached aspects
- **Lazy fetch**: O(1) cache lookup + O(1) network if miss

## Extension Points

SDK V2 designed for extensibility:

1. **New entity types** - Extend `Entity` base class
2. **Custom aspects** - Use `getAspectLazy` / `getOrCreateAspect`
3. **New patch types** - Leverage existing patch builders
4. **Custom caching** - Override `cacheTtlMs`
5. **Transport customization** - Customize RestEmitter via builder

## Summary

Java SDK V2 achieves its goals through principled design:

- **Reuse over reinvention** - Leverages existing patch builders and RestEmitter
- **Patches over replacements** - Efficient incremental updates
- **Lazy over eager** - Aspects fetched on-demand with caching
- **Type safety over convenience** - Strong typing throughout
- **Layers over monoliths** - Clear separation of entity, operations, transport
- **Pragmatism over purity** - Mode-aware behavior matches real-world usage

The result is an SDK that feels natural to Java developers while providing the efficiency and correctness required for production metadata management at scale.
