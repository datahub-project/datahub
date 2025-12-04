# DataHub Java SDK V2 Design Document

## Executive Summary

This document describes the design of DataHub Java SDK V2, a modern, user-friendly Java client library that provides feature parity with the Python SDK V2. The new SDK addresses feedback from enterprise Java customers who require a first-class SDK experience comparable to Python developers.

This document is organized into two main sections:

- **Part 1 - User-Facing API Design**: The public API, patterns, and behaviors visible to SDK users
- **Part 2 - Developer-Facing Implementation**: Internal architecture and implementation details for contributors

> **Why Hand-Crafted?** For a deep dive into why we chose to hand-craft this SDK instead of using OpenAPI code generation, see [Java SDK V2 Philosophy](java-sdk-v2-philosophy.md).

## Background

### Problem Statement

Currently, DataHub's Java SDK (`datahub-client`) provides only low-level emission capabilities:

- Manual MCP (Metadata Change Proposal) construction required
- No high-level entity builders for Dataset, Chart, Dashboard, etc.
- No client for CRUD operations (read, update, delete)
- No patch capabilities for granular updates
- Significantly inferior developer experience compared to Python SDK V2

This gap has created issues with enterprise customers, particularly Java shops who feel like "second-class citizens" when compared to Python developers.

### Goals

1. **Feature Parity**: Match Python SDK V2 capabilities for entity management
2. **Backward Compatibility**: Maintain 100% compatibility with existing Java SDK
3. **Namespace Separation**: Use `datahub.client.v2.*` namespace for new APIs
4. **Builder Pattern**: Fluent, type-safe API for entity construction
5. **Patch Support**: Granular updates without full entity replacement
6. **CRUD Operations**: Support create, read, update, upsert operations (delete/exists deferred)
7. **Comprehensive Testing**: Unit and integration tests validating all functionality

### Non-Goals

- Rewriting existing emitter infrastructure (leverage existing)
- 100% feature parity with Python SDK (focus on core entities first)
- GraphQL client implementation (focus on REST/OpenAPI)
- Search client (future enhancement)
- Lineage client (future enhancement)

---

# Part 1: User-Facing API Design

This section describes the public API that SDK users interact with - the patterns, behaviors, and interfaces that define the developer experience.

## Design Principles

### 1. Fluent Builder Pattern

Intuitive entity construction through method chaining:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .env("PROD")
    .description("My dataset")
    .build();

// Fluent metadata operations with type-safe method chaining
dataset.addTag("pii")
       .addOwner("urn:li:corpuser:jdoe", OwnershipType.TECHNICAL_OWNER)
       .setDomain("urn:li:domain:Analytics")
       .setStructuredProperty("io.acryl.dataQuality.qualityScore", 95.5);

client.entities().upsert(dataset);
```

### 2. Type Safety and Compile-Time Checking

Leverage Java's strong typing:

- Strongly-typed URNs (`DatasetUrn`, `ChartUrn`, etc.)
- Generic types for entity operations
- CRTP (Curiously Recurring Template Pattern) for type-safe mixin interfaces
- Builder validation at construction time

### 3. Mode-Aware Behavior

**SDK Mode vs INGESTION Mode** for proper separation of concerns:

- **SDK Mode (default)**: User edits → `editableDatasetProperties`
- **INGESTION Mode**: Pipeline writes → `datasetProperties`
- Getters intelligently prefer editable aspects over system aspects

```java
// SDK mode - user edits go to editable aspects
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .mode(OperationMode.SDK)  // Default
    .build();

// INGESTION mode - pipeline writes go to system aspects
DataHubClientV2 ingestionClient = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .mode(OperationMode.INGESTION)
    .build();
```

### 4. Patch-First Philosophy

**Design Decision: Prioritize patches over full aspect replacement**

The SDK V2 is designed around patch-based operations because they represent the most common and intuitive way to make metadata changes:

```java
Dataset dataset = client.entities().get(datasetUrn);
Dataset mutable = dataset.mutable();  // Get mutable copy

// These create patches internally - no server calls yet
mutable.addTag("pii")
       .addTag("sensitive")
       .addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER);

// Single call emits all accumulated patches atomically
client.entities().update(mutable);
```

**Why patches?**

- **Simplicity**: Users think "add a tag" not "fetch all tags, add one, PUT entire tag aspect back"
- **Safety**: Patches don't overwrite concurrent changes from other users
- **Efficiency**: Only changed fields are transmitted and processed
- **Common use case**: Most metadata operations are incremental additions/removals

**When to use low-level SDK:**
If you need to completely replace an aspect (full PUT/upsert semantics), use the V1 SDK's `RestEmitter` directly with `MetadataChangeProposalWrapper`. The V2 SDK focuses on making common operations simple, not exposing every low-level primitive.

### 5. Composition Through Mixin Interfaces

Shared metadata operations via type-safe mixin interfaces:

- `HasTags<T>` - Add, remove, set tags
- `HasOwners<T>` - Manage ownership
- `HasGlossaryTerms<T>` - Associate glossary terms
- `DomainOperations<T>` - Domain assignment
- `HasContainer<T>` - Parent-child hierarchies

All mixins use CRTP pattern for type-safe method chaining that returns the concrete entity type.

## Architecture

### Package Structure (Actual Implementation)

```
datahub-client/
├── src/main/java/
│   ├── datahub/client/                    # Existing v1 (unchanged)
│   │   ├── Emitter.java
│   │   ├── rest/RestEmitter.java
│   │   └── ...
│   │
│   └── datahub/client/v2/                 # New v2 namespace
│       ├── DataHubClientV2.java           # Main client entry point
│       │
│       ├── entity/                        # Entity classes
│       │   ├── Entity.java                # Base entity class (490 lines)
│       │   ├── AspectCache.java           # Unified cache with dirty tracking (184 lines)
│       │   ├── CachedAspect.java          # Aspect wrapper with metadata (68 lines)
│       │   ├── AspectSource.java          # SERVER vs LOCAL enum (23 lines)
│       │   ├── ReadMode.java              # ALLOW_DIRTY vs SERVER_ONLY (28 lines)
│       │   ├── Dataset.java               # Dataset entity (564 lines)
│       │   ├── Chart.java                 # Chart entity (587 lines)
│       │   ├── Dashboard.java             # Dashboard entity (671 lines)
│       │   ├── DataJob.java               # DataJob entity (597 lines)
│       │   ├── DataFlow.java              # DataFlow entity (467 lines)
│       │   ├── Container.java             # Container entity (500 lines)
│       │   ├── MLModel.java               # ML Model entity NEW
│       │   ├── MLModelGroup.java          # ML Model Group entity NEW
│       │   ├── HasTags.java               # Tag operations mixin
│       │   ├── HasOwners.java             # Ownership operations mixin
│       │   ├── HasGlossaryTerms.java      # Terms operations mixin
│       │   ├── HasDomains.java            # Domain operations mixin
│       │   ├── HasContainer.java          # Container hierarchy mixin
│       │   └── HasStructuredProperties.java # Structured properties mixin
│       │
│       ├── operations/                    # CRUD operation clients
│       │   └── EntityClient.java          # Entity CRUD operations (570 lines)
│       │
│       └── config/                        # Configuration
│           └── DataHubClientConfigV2.java # Config with mode support
│
└── src/test/java/                         # Tests mirror structure
    └── datahub/client/v2/
        ├── DataHubClientV2Test.java       # Client tests
        ├── entity/                        # 378 unit tests
        │   ├── AspectCacheTest.java       # 30 tests (cache infrastructure)
        │   ├── CachedAspectTest.java      # 13 tests (cache infrastructure)
        │   ├── DatasetTest.java           # 37 tests
        │   ├── ChartTest.java             # 43 tests
        │   ├── DashboardTest.java         # 52 tests
        │   ├── DataJobTest.java           # 45 tests
        │   ├── DataFlowTest.java          # 40 tests
        │   ├── ContainerTest.java         # 40 tests
        │   ├── MLModelTest.java           # 44 tests
        │   └── MLModelGroupTest.java      # 38 tests
        └── integration/                   # 79 integration tests
            ├── DatasetIntegrationTest.java
            ├── ChartIntegrationTest.java
            ├── DashboardIntegrationTest.java
            ├── DataJobIntegrationTest.java
            ├── DataFlowIntegrationTest.java
            ├── ContainerIntegrationTest.java
            ├── MLModelIntegrationTest.java
            └── MLModelGroupIntegrationTest.java
```

**Key Design Decisions:**

- No separate `patch/` package - patches accumulate internally within entities
- Mixin interfaces in `entity/` package using CRTP pattern for type safety
- Support for 8 entity types including ML entities (MLModel, MLModelGroup)
- Mode-aware configuration for SDK vs INGESTION behavior

### Core Classes

#### 1. DataHubClientV2 (Main Entry Point)

**File**: `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/DataHubClientV2.java` (266 lines)

```java
package datahub.client.v2;

/**
 * Main entry point for DataHub Java SDK V2.
 * Provides high-level operations for entity management with mode-aware behavior.
 *
 * <p>Example usage:
 * <pre>
 * DataHubClientV2 client = DataHubClientV2.builder()
 *     .server("http://localhost:8080")
 *     .token("my-token")
 *     .mode(OperationMode.SDK)  // SDK or INGESTION mode
 *     .build();
 *
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .env("PROD")
 *     .description("My dataset")
 *     .build();
 *
 * client.entities().upsert(dataset);
 * </pre>
 */
public class DataHubClientV2 implements AutoCloseable {
    private final RestEmitter emitter;
    private final DataHubClientConfigV2 config;
    private final EntityClient entityClient;

    // Builder for client configuration
    public static Builder builder() { ... }

    // Entity operations
    public EntityClient entities() { return entityClient; }

    // Low-level emitter access (for advanced users)
    public RestEmitter emitter() { return emitter; }

    // Configuration access
    public DataHubClientConfigV2 config() { return config; }

    @Override
    public void close() throws IOException { ... }

    public static class Builder {
        public Builder server(String serverUrl) { ... }
        public Builder token(String token) { ... }
        public Builder timeout(int timeoutMs) { ... }
        public Builder mode(OperationMode mode) { ... }  // NEW
        public Builder config(DataHubClientConfigV2 config) { ... }
        public DataHubClientV2 build() { ... }
    }
}
```

**Design Features:**

- Mode-aware behavior (SDK vs INGESTION) for proper aspect routing
- Environment variable support for configuration
- Builder pattern with sensible defaults
- AutoCloseable interface for resource management

#### 2. Entity (Base Class) - User-Facing API

**File**: `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/Entity.java` (490 lines)

The Entity base class provides a unified interface for all DataHub entities. From a user perspective, all entities support:

**Public API Methods:**

```java
// URN access
public Urn getUrn()
public abstract String getEntityType()

// Convert to MCPs for emission (primarily internal)
public List<MetadataChangeProposalWrapper> toMCPs()
```

**Entity Construction:**

Entities are constructed via fluent builders:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .env("PROD")
    .description("My dataset")
    .build();
```

**Fluent Metadata Operations:**

All entities support method chaining for metadata operations (via mixin interfaces):

```java
dataset.addTag("pii")
       .addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER)
       .setDomain(domainUrn)
       .addTerm(termUrn);
```

**Lazy Loading:**

Entities loaded from the server fetch aspects on-demand:

```java
Dataset dataset = client.entities().get(datasetUrn);  // Only URN loaded
String description = dataset.getDescription();         // Aspect fetched now
List<String> tags = dataset.getTags();                // Another aspect fetch
```

**Patch Accumulation:**

Metadata operations create patches that accumulate until save:

```java
Dataset dataset = client.entities().get(datasetUrn);
Dataset mutable = dataset.mutable();  // Get mutable copy
mutable.addTag("pii");           // Creates patch (not sent yet)
mutable.addTag("sensitive");     // Another patch (not sent yet)
client.entities().update(mutable); // Emits all patches atomically
```

**Immutability-by-Default:**

Entities fetched from the server are read-only to prevent accidental mutations:

```java
Dataset dataset = client.entities().get(datasetUrn);
dataset.isReadOnly();  // true
dataset.isMutable();   // false

// Attempting mutation throws ReadOnlyEntityException
// dataset.addTag("pii");  // ERROR!

// Get mutable copy for updates
Dataset mutable = dataset.mutable();
mutable.isMutable();  // true
mutable.addTag("pii");  // Works
client.entities().upsert(mutable);
```

**Entity Lifecycle:**

1. **Builder-created entities** - Mutable from creation

   ```java
   Dataset dataset = Dataset.builder()
       .platform("snowflake")
       .name("my_table")
       .build();
   dataset.isMutable();  // true - can mutate immediately
   ```

2. **Server-fetched entities** - Immutable by default

   ```java
   Dataset dataset = client.entities().get(urn);
   dataset.isReadOnly();  // true - must call .mutable()
   ```

3. **Mutable copies** - Created via `.mutable()`
   ```java
   Dataset mutable = dataset.mutable();
   mutable.isMutable();  // true - can mutate
   ```

**The .mutable() method:**

- Creates a shallow copy with independent mutability flags
- Shares aspect cache with original (read-your-own-writes semantics)
- Idempotent - returns self if already mutable
- Original entity remains read-only after creating mutable copy

**Why immutability-by-default?**

- Makes mutations explicit and intentional
- Prevents accidental modification when passing entities between functions
- Clear separation between read and write workflows
- Enables safe entity sharing across threads
- Common pattern in modern APIs (Rust, Python, Java immutable collections)

See "Developer-Facing Implementation Design" section below for internal architecture details.

#### 3. Supported Entities

The SDK V2 implements 8 entity types with full metadata support:

**Data Entities:**

- **Dataset** - Tables, views, files with schema support
- **Container** - Databases, schemas, folders (hierarchical structures)

**Pipeline Entities:**

- **DataFlow** - Pipelines, workflows (Airflow DAGs, Spark jobs, dbt projects)
- **DataJob** - Individual tasks with inlet/outlet lineage

**Visualization Entities:**

- **Chart** - Visualizations with input dataset lineage
- **Dashboard** - Dashboards with chart relationships and input datasets

**ML Entities:**

- **MLModel** - Machine learning models with metrics, hyperparameters, training jobs
- **MLModelGroup** - Model families with version management

**Common Entity Operations:**

All entities support these fluent operations (via mixin interfaces):

```java
// Tags
entity.addTag("pii")
      .removeTag("deprecated")
      .setTags(Arrays.asList("tag1", "tag2"))
      .clearTags()

// Owners
entity.addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER)
      .removeOwner(ownerUrn)
      .setOwners(ownerList)
      .clearOwners()

// Glossary Terms
entity.addTerm(termUrn)
      .removeTerm(termUrn)
      .setTerms(termList)
      .clearTerms()

// Domains
entity.setDomain(domainUrn)
      .removeDomain(domainUrn)
      .clearDomains()

// Container (for hierarchical entities)
entity.setContainer(containerUrn)
      .clearContainer()

// Structured Properties (custom typed metadata)
entity.setStructuredProperty("io.acryl.dataManagement.replicationSLA", "24h")
      .setStructuredProperty("io.acryl.dataQuality.qualityScore", 95.5)
      .setStructuredProperty("io.acryl.dataManagement.certifications",
                             Arrays.asList("SOC2", "HIPAA", "GDPR"))
      .setStructuredProperty("io.acryl.privacy.retentionDays", 90, 180, 365)
      .removeStructuredProperty("io.acryl.dataManagement.deprecated")
```

**Entity-Specific Documentation:**

See comprehensive guides in `metadata-integration/java/docs/sdk-v2/`:

- `dataset-entity.md` - Dataset with schema support
- `chart-entity.md` - Chart with lineage
- `dashboard-entity.md` - Dashboard with chart relationships
- `container-entity.md` - Container hierarchies
- `dataflow-entity.md` - DataFlow pipelines
- `datajob-entity.md` - DataJob with inlet/outlet lineage
- `mlmodel-entity.md` - MLModel with metrics
- `mlmodelgroup-entity.md` - MLModelGroup with versions

#### 4. EntityClient (CRUD Operations)

**File**: `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/operations/EntityClient.java` (570 lines)

```java
package datahub.client.v2.operations;

/**
 * Client for entity CRUD operations.
 * Provides create, read, update, and upsert operations.
 */
public class EntityClient {
    private final RestEmitter emitter;
    private final DataHubClientConfigV2 config;

    /**
     * Create a new entity (convenience method - same as upsert).
     */
    public <T extends Entity> void create(T entity) throws IOException, ExecutionException, InterruptedException {
        upsert(entity);
    }

    /**
     * Upsert an entity (create or update).
     * Emits all aspects and accumulated patches.
     */
    public <T extends Entity> void upsert(T entity) throws IOException, ExecutionException, InterruptedException {
        List<MetadataChangeProposalWrapper> mcps = entity.toMCPs();
        // Emit all MCPs asynchronously and wait for completion
        // ...
    }

    /**
     * Update an existing entity.
     * Emits only accumulated patches (not full aspects).
     */
    public <T extends Entity> void update(T entity) throws IOException, ExecutionException, InterruptedException {
        // Emit only pending patches
        // ...
    }

    /**
     * Get an entity by URN.
     * Returns entity with lazy-loaded aspects.
     */
    public <T extends Entity> T get(Urn urn, Class<T> entityClass) throws IOException {
        // Fetch entity aspects from server
        // Construct entity with lazy loading support
        // ...
    }

    // Note: delete(Urn) and exists(Urn) operations deferred to future releases
}
```

**Supported Operations:**

- `create()` - Create new entities (wrapper for upsert)
- `upsert()` - Create or update entities (emits all aspects + patches)
- `update()` - Update existing entities (emits only patches)
- `get()` - Retrieve entities with lazy loading
- `delete()` and `exists()` - Deferred to future releases

**Patch Behavior:**

Patches are accumulated **inside entities** during metadata operations and emitted automatically during `upsert()`/`update()`:

```java
Dataset dataset = client.entities().get(datasetUrn);
Dataset mutable = dataset.mutable();  // Get mutable copy
mutable.addTag("pii");           // Creates internal patch
mutable.addTag("sensitive");     // Creates another internal patch
client.entities().update(mutable); // Emits both patches atomically
```

There is **no separate `patch()` method** - patches are managed internally by entities.

#### 5. Mixin Interfaces (CRTP Pattern)

**Files**: `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/Has*.java`

Mixin interfaces provide reusable metadata operations across entities using the **Curiously Recurring Template Pattern (CRTP)** for type-safe method chaining:

```java
/**
 * Interface for entities that support tags.
 * Uses CRTP for type-safe method chaining.
 */
public interface HasTags<T extends Entity & HasTags<T>> {

    /**
     * Add a tag to this entity.
     * Creates a patch that will be emitted on save.
     */
    default T addTag(@Nonnull String tagUrn) {
        // Implementation creates patch internally
        return (T) this;
    }

    default T removeTag(@Nonnull String tagUrn) { ... }
    default T setTags(@Nonnull List<String> tagUrns) { ... }
    default T clearTags() { ... }

    // Getter methods
    default List<String> getTags() { ... }
}
```

**Available Mixin Interfaces:**

1. **`HasTags<T>`** - Tag operations (`addTag`, `removeTag`, `setTags`, `clearTags`)
2. **`HasOwners<T>`** - Ownership operations (`addOwner`, `removeOwner`, `setOwners`, `clearOwners`)
3. **`HasGlossaryTerms<T>`** - Glossary term operations (`addTerm`, `removeTerm`, `setTerms`, `clearTerms`)
4. **`DomainOperations<T>`** - Domain operations (`setDomain`, `removeDomain`, `clearDomains`)
5. **`HasContainer<T>`** - Container hierarchy (`setContainer`, `clearContainer`)
6. **`HasStructuredProperties<T>`** - Structured properties operations (`setStructuredProperty`, `removeStructuredProperty`)

**Why CRTP?**

The CRTP pattern enables type-safe method chaining that returns the concrete entity type:

```java
// Without CRTP: returns Entity
Entity entity = dataset.addTag("pii");  // Loses Dataset type!

// With CRTP: returns Dataset
Dataset dataset = dataset.addTag("pii")
                         .addOwner(ownerUrn, type)  // Still Dataset type!
                         .setDomain(domainUrn);     // Still Dataset type!
```

**Entity Implementations:**

Entities implement mixin interfaces by declaring them in the class signature:

```java
public class Dataset extends Entity
    implements HasTags<Dataset>,
               HasOwners<Dataset>,
               HasGlossaryTerms<Dataset>,
               DomainOperations<Dataset>,
               HasContainer<Dataset>,
               HasStructuredProperties<Dataset> {
    // Mixin methods provided by default implementations
}
```

---

# Part 2: Developer-Facing Implementation Design

This section describes the internal architecture and implementation details for developers contributing to the SDK.

## Internal Architecture

### Entity Base Class - Internal Implementation

**File**: `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/Entity.java` (490 lines)

The Entity base class implements three core subsystems:

#### 1. AspectCache System with Read-Your-Own-Writes

**Unified Cache Architecture**: The SDK uses a unified `AspectCache` that provides read-your-own-writes semantics with proper dirty tracking. This architecture fixes bugs where fetched aspects would override patches.

**Core Implementation Files:**

- `AspectCache.java` (184 lines) - Main cache with dirty tracking
- `CachedAspect.java` (68 lines) - Aspect wrapper with metadata
- `AspectSource.java` (23 lines) - Enum for SERVER vs LOCAL aspects
- `ReadMode.java` (28 lines) - Enum for ALLOW_DIRTY vs SERVER_ONLY reads

**Key Architectural Features:**

1. **AspectSource Tracking**: Distinguishes between SERVER-fetched aspects (subject to TTL) and LOCAL-created aspects (no expiration)

2. **Dirty Tracking**: Explicit marking of aspects that need write-back to server via `markDirty()` method

3. **Read-Your-Own-Writes**: Default `ReadMode.ALLOW_DIRTY` returns local modifications immediately, `SERVER_ONLY` mode skips dirty aspects

4. **TTL Management**: 60-second TTL enforced only for SERVER-sourced aspects, LOCAL aspects never expire

5. **Thread Safety**: Uses `ConcurrentHashMap` for safe concurrent access

**Internal State (Entity.java):**

```
protected final AspectCache cache;  // Unified cache with dirty tracking
protected final Map<String, List<MetadataChangeProposal>> pendingPatches;
private DataHubClientV2 boundClient = null;
```

**Cache Operations:**

- `getAspectLazy()` - Lazy loads from server, stores as clean SERVER-sourced aspect
- `getOrCreateAspect()` - Gets from cache or creates new LOCAL-sourced aspect (marked dirty)
- `markAspectDirty()` - Marks aspect dirty after in-place modification (used by domain operations)
- `toMCPs()` - Returns **only dirty aspects** for emission (excludes clean fetched aspects)

**Why This Architecture?**

The unified cache solves a critical bug: when entities are fetched from the server and then patch operations are applied (e.g., `removeTerm()`), the cached aspect would be included in `toMCPs()` and override the patches. With dirty tracking, `toMCPs()` only returns modified aspects, allowing patches to work correctly.

#### 2. Patch Accumulation and MCP Generation

Metadata operations create patches that accumulate until emission. The system supports two types of operations:

**Patch-Based Operations** (incremental updates):

- Tags, owners, glossary terms use `PatchBuilder` classes
- Patches accumulate in `pendingPatches` map (aspect name → list of patches)
- Multiple operations on same aspect create multiple patches

**Cache-Based Operations** (full aspect replacement):

- Domains, custom properties modify aspects in cache
- Aspects marked dirty via `markAspectDirty()` after modification
- Dirty aspects included in `toMCPs()` output

**MCP Generation:**

The `toMCPs()` method returns **only dirty aspects** and accumulated patches:

```
public List<MetadataChangeProposalWrapper> toMCPs() {
    // 1. Add dirty aspects from cache (excludes clean fetched aspects)
    for (Map.Entry<String, RecordTemplate> entry : cache.getDirtyAspects().entrySet()) {
        mcps.add(createMCP(entry.getKey(), entry.getValue()));
    }

    // 2. Add accumulated patches
    for (PatchBuilder builder : patchBuilders.values()) {
        mcps.add(builder.build());
    }

    // 3. Add pending MCPs
    mcps.addAll(pendingMCPs);

    return mcps;
}
```

**Critical Design Point**: `toMCPs()` uses `cache.getDirtyAspects()` instead of all cached aspects. This ensures that fetched aspects don't override patches - only locally modified aspects are emitted.

#### 3. Mode-Aware Aspect Routing

SDK mode vs INGESTION mode for proper aspect selection:

````java
/**
 * Get aspect name based on operation mode.
 * SDK mode: prefer editable aspects
 * INGESTION mode: use system aspects
 */
protected String getAspectName(Class<? extends RecordTemplate> aspectClass, OperationMode mode) {
    if (mode == OperationMode.SDK) {
        // Check if editable variant exists
        String editableAspectName = getEditableAspectName(aspectClass);
        if (editableAspectName != null) {
            return editableAspectName;
        }
    }
    return aspectClass.getSimpleName();
}

/**
 * Get getter preference order: editable aspects first, then system aspects.
 */
protected <T extends RecordTemplate> T getAspectWithPreference(
    Class<T> editableClass,
    Class<T> systemClass
) {
    // Try editable aspect first
    T editable = getAspectLazy(editableClass);
    if (editable != null) {
        return editable;
    }

    // Fall back to system aspect
    return getAspectLazy(systemClass);
}

## Implementation Phases

### Phase 1: Core Framework

Base functionality for all entities:

- Base `Entity` class with aspect management, lazy loading, and patch accumulation
- `DataHubClientV2` main client class with mode-aware behavior
- `EntityClient` with create, read, update, upsert operations
- Configuration classes with environment variable support
- Mixin interfaces using CRTP pattern for type safety

### Phase 2: Dataset Entity

Reference implementation demonstrating all patterns:

- `Dataset` entity with fluent builder
- Dataset-specific aspects (properties, schema, lineage)
- Mixin interface implementations
- Comprehensive unit tests

### Phase 3: Additional Entities

Seven additional entity types:

- `Chart` - Visualizations with lineage
- `Dashboard` - Dashboards with chart relationships
- `Container` - Hierarchical data structures
- `DataJob` - Pipeline tasks with inlet/outlet lineage
- `DataFlow` - Pipeline workflows
- `MLModel` - Machine learning models
- `MLModelGroup` - ML model families

### Phase 4: Patch Capabilities

Patch-based updates for efficient metadata changes:

- Internal patch accumulation within entities (not separate patch builders)
- Automatic patch emission on `update()` and `upsert()`
- Leverages existing `PatchBuilder` classes from entity-registry module
- Patches tested via entity unit tests

### Phase 5: Testing & Documentation

Comprehensive validation and user guides:

- Integration tests with live DataHub server
- API documentation (Javadoc) and 13 comprehensive Markdown guides
- 19 working example files demonstrating real-world usage
- Migration guide from V1
- Design principles document
- Patch operations deep-dive
- Entity-specific guides for all 8 entities

## Testing Strategy

### Unit Tests

Each entity and component has comprehensive unit tests:

- Builder validation (required fields, optional fields, validation logic)
- Aspect management (getters, setters, mode-aware routing)
- MCP generation (full aspects + patches)
- Patch operations (accumulation, emission)
- Fluent API chaining (type safety via CRTP)
- Mixin operations (tags, owners, terms, domains)

**Test Coverage by Entity:**
- Dataset: 37 tests
- Chart: 43 tests
- Dashboard: 52 tests
- DataJob: 45 tests
- DataFlow: 40 tests
- Container: 40 tests
- MLModel: 44 tests
- MLModelGroup: 38 tests

### Integration Tests

Full end-to-end tests against a real DataHub instance:

```java
@Test
public void testDatasetCreateAndRead() throws Exception {
    // Create client
    DataHubClientV2 client = DataHubClientV2.builder()
        .server(TEST_SERVER)
        .token(TEST_TOKEN)
        .build();

    // Create dataset
    Dataset dataset = Dataset.builder()
        .platform("snowflake")
        .name("db.schema.test_table_" + System.currentTimeMillis())
        .env("PROD")
        .description("Test dataset created by Java SDK V2")
        .build();

    dataset.addTag("test-tag")
           .addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);

    // Upsert
    client.entities().upsert(dataset);

    // Read back
    Dataset retrieved = client.entities().get(dataset.getUrn(), Dataset.class);
    assertNotNull(retrieved);
    assertEquals("Test dataset created by Java SDK V2", retrieved.getDescription());
}

@Test
public void testDatasetPatchOperations() throws Exception {
    DataHubClientV2 client = DataHubClientV2.builder()
        .server(TEST_SERVER)
        .token(TEST_TOKEN)
        .build();

    // Create dataset first
    Dataset dataset = Dataset.builder()
        .platform("snowflake")
        .name("db.schema.test_table_patch_" + System.currentTimeMillis())
        .env("PROD")
        .build();
    client.entities().upsert(dataset);

    // Retrieve and apply patches
    Dataset retrieved = client.entities().get(dataset.getUrn(), Dataset.class);
    Dataset mutable = retrieved.mutable();  // Get mutable copy
    mutable.addTag("pii")                    // Creates patch
           .addTag("sensitive")              // Another patch
           .addTerm("urn:li:glossaryTerm:CustomerData");  // Another patch

    // All patches emitted atomically
    client.entities().update(mutable);

    // Verify patches were applied
    Dataset verified = client.entities().get(dataset.getUrn(), Dataset.class);
    assertTrue(verified.getTags().contains("urn:li:tag:pii"));
}
````

**Integration Test Coverage:**

- Entity creation and retrieval
- Tag, owner, term, domain operations
- Lineage relationships (charts → datasets, jobs → datasets)
- Custom properties
- Full metadata workflows
- Batch operations
- Patch accumulation and emission

**Running Integration Tests:**

```bash
export DATAHUB_SERVER=http://localhost:8080
export DATAHUB_TOKEN=your_token

./gradlew :metadata-integration:java:datahub-client:test --tests "*Integration*"
```

### Test Coverage Results

- Unit test coverage: **>80%** for new code (378 unit tests + 79 integration tests = 457 total)
- All public APIs covered
- Edge cases tested (null values, invalid inputs, mode switching)
- Async operations tested with proper synchronization
- Cache infrastructure thoroughly tested (43 tests for AspectCache + CachedAspect)
- Full end-to-end integration tests (79 tests)

## API Documentation

All public classes and methods have comprehensive Javadoc plus extensive Markdown documentation:

**Javadoc Coverage:**

- Class-level documentation explaining purpose and usage
- Method-level documentation with parameters, returns, exceptions
- Code examples for common use cases
- Links to related classes and methods

**Markdown Documentation (13 files):**

Located in `metadata-integration/java/docs/sdk-v2/`:

1. **getting-started.md** - Quick start guide for new users
2. **design-principles.md** - Architecture and design decisions
3. **dataset-entity.md** - Dataset operations and schema support
4. **chart-entity.md** - Chart operations and lineage
5. **dashboard-entity.md** - Dashboard operations and relationships
6. **container-entity.md** - Container hierarchies
7. **dataflow-entity.md** - DataFlow pipeline operations
8. **datajob-entity.md** - DataJob inlet/outlet lineage
9. **mlmodel-entity.md** - MLModel metrics and hyperparameters
10. **mlmodelgroup-entity.md** - MLModelGroup version management
11. **patch-operations.md** - Deep dive into patch-based updates
12. **migration-from-v1.md** - Migration guide from V1 SDK
13. **java-sdk-v2-design.md** - This comprehensive design document

**Working Examples (19 files):**

Located in `metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/`:

- Dataset examples: DatasetCreateExample, DatasetFullExample, DatasetPatchExample
- Chart examples: ChartCreateExample, ChartFullExample, ChartLineageExample
- Dashboard examples: DashboardCreateExample, DashboardFullExample, DashboardLineageExample
- DataFlow examples: DataFlowCreateExample, DataFlowFullExample
- DataJob examples: DataJobCreateExample, DataJobFullExample, DataJobLineageExample
- Container examples: ContainerCreateExample, ContainerFullExample, ContainerHierarchyExample
- MLModel examples: MLModelCreateExample, MLModelFullExample
- MLModelGroup examples: MLModelGroupCreateExample, MLModelGroupFullExample

## Migration Guide

For users of the existing Java SDK:

### Before (V1):

```java
RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080"));

DatasetUrn urn = new DatasetUrn(
    new DataPlatformUrn("postgres"),
    "my_table",
    FabricType.PROD
);

DatasetProperties props = new DatasetProperties();
props.setDescription("My dataset");

MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
    .entityType("dataset")
    .entityUrn(urn)
    .upsert()
    .aspect(props)
    .build();

emitter.emit(mcpw).get();
```

### After (V2):

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .build();

Dataset dataset = Dataset.builder()
    .platform("postgres")
    .name("my_table")
    .description("My dataset")
    .build();

client.entities().upsert(dataset);
```

## Decision Log

### 1. Use Pegasus Models vs OpenAPI Models

**Decision**: Use Pegasus models (`com.linkedin.*`) for aspect classes.

**Rationale**:

- Pegasus models are the canonical representation in DataHub
- Already used by v1 SDK, maintains consistency
- Generated from PDL schemas, always in sync with backend
- OpenAPI models are less mature and have fewer utilities

**Result**: Proven correct - seamless integration with existing infrastructure.

### 2. Namespace Separation

**Decision**: Use `datahub.client.v2.*` namespace.

**Rationale**:

- Clear separation from v1 API
- Allows side-by-side usage
- Follows semantic versioning principles
- Easy to deprecate v1 in future

**Result**: 100% backward compatibility achieved - v1 code unchanged.

### 3. Builder Pattern

**Decision**: Use nested static Builder classes.

**Rationale**:

- Idiomatic Java pattern
- Type-safe construction
- Optional parameters handled cleanly
- Better than telescoping constructors

**Result**: Excellent developer experience with fluent API.

### 4. Synchronous vs Async

**Decision**: Provide synchronous API that wraps async operations.

**Rationale**:

- Simpler for most users
- Matches Python SDK V2 API
- Can expose async API later for advanced users
- RestEmitter already provides async primitives

**Result**: Simplified API widely adopted in examples and tests.

### 5. Error Handling

**Decision**: Throw checked exceptions for I/O operations.

**Rationale**:

- Forces callers to handle errors
- Consistent with Java conventions
- Clear distinction between programmer errors and runtime failures

**Result**: Clear error handling patterns in all code.

**Exception Hierarchy:**

The SDK introduces custom exceptions for common error conditions:

**ReadOnlyEntityException** - Thrown when attempting to mutate a read-only entity:

```java
try {
  Dataset dataset = client.entities().get(urn);
  dataset.addTag("pii");  // Throws ReadOnlyEntityException
} catch (ReadOnlyEntityException e) {
  // Exception message explains the issue and provides fix
  System.err.println(e.getMessage());

  // Fix: Get mutable copy first
  Dataset mutable = dataset.mutable();
  mutable.addTag("pii");
  client.entities().upsert(mutable);
}
```

**PendingMutationsException** - Thrown when reading from entity with pending mutations:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.setDescription("New description");
// dataset.getDescription();  // Throws PendingMutationsException!

// Fix: Save first, then read
client.entities().upsert(dataset);  // Clears dirty flag
String desc = dataset.getDescription();  // Now works
```

**Why these restrictions?**

- **ReadOnlyEntityException**: Makes mutations explicit, prevents accidental changes when passing entities between functions
- **PendingMutationsException**: Prevents reading stale cached data, enforces explicit save-then-fetch workflow

Both restrictions enforce clear separation between read and write workflows. These may be relaxed in future versions as the API matures and usage patterns emerge.

### 6. Patch-First over Full Aspect Replacement

**Decision**: Prioritize patch-based operations as the primary API, defer full aspect replacement to V1 SDK.

**Rationale**:

- **User mental model**: "Add a tag" is more natural than "fetch all tags, modify list, PUT entire aspect"
- **Safety**: Patches don't clobber concurrent changes from other users/systems
- **Simplicity**: Most metadata operations are incremental (add owner, remove tag, etc.)
- **Efficiency**: Only changed fields transmitted and processed by server
- **Escape hatch exists**: Users needing full PUT semantics can use V1 SDK's `RestEmitter` directly

**Why not both?**
V2 SDK focuses on making common operations simple, not exposing every low-level primitive. This keeps the API focused and prevents confusion about when to use patches vs full replacement.

**Result**: Clean, intuitive API for 95% of use cases. Power users can drop to V1 SDK for remaining 5%.

### 7. Internal Patch Accumulation vs External Patch Builders

**Decision**: Accumulate patches **inside entities** rather than separate patch builder classes.

**Rationale**:

- More intuitive API - metadata operations just work
- Patches automatically emitted on save
- Reduces API surface area
- Simplifies user code

**Original Design**: Separate `DatasetPatch`, `ChartPatch` builder classes

**Actual Implementation**: Patches accumulate in `Entity.pendingPatches` and emit via `toMCPs()`

**Result**: Superior developer experience - no need to learn separate patch API.

### 8. CRTP Pattern for Mixin Interfaces

**Decision**: Use Curiously Recurring Template Pattern for type-safe mixin interfaces.

**Rationale**:

- Type-safe method chaining returns concrete entity type
- Compile-time type checking
- No casting required in user code
- Idiomatic Java generics pattern

**Original Design**: Simple interfaces returning `Entity`

**Actual Implementation**:

```java
public interface HasTags<T extends Entity & HasTags<T>> {
    default T addTag(String tagUrn) { return (T) this; }
}
```

**Result**: Excellent type safety and developer experience.

### 9. Mode-Aware Behavior (SDK vs INGESTION)

**Decision**: Support SDK mode and INGESTION mode for aspect routing.

**Rationale**:

- Proper separation of user edits vs pipeline writes
- SDK mode → editable aspects (user overrides)
- INGESTION mode → system aspects (pipeline data)
- Getters prefer editable over system

**Original Design**: Not specified

**Actual Implementation**: `OperationMode` enum with aspect routing logic

**Result**: Clear separation of concerns, aligns with DataHub's aspect model.

### 10. Lazy Loading for GET Operations

**Decision**: Implement lazy loading for aspects when entities are retrieved.

**Rationale**:

- Performance - only fetch aspects when accessed
- Client binding enables on-demand fetching
- Cache management with timestamps

**Original Design**: Not specified (GET deferred)

**Actual Implementation**: Full lazy loading with `getAspectLazy()` and client binding

**Result**: Efficient entity retrieval with on-demand aspect fetching.

## Design Questions and Resolutions

1. **GET operation implementation**: Should we implement REST client for reading entities, or defer to future?

   - **Resolution**: Implemented with lazy loading support

2. **Search client**: Should we include search functionality in V2?

   - **Resolution**: Deferred to future (out of scope for V2)

3. **Lineage client**: Should we include lineage management?

   - **Resolution**: Basic lineage on Dataset, Chart, Dashboard, DataJob entities

4. **Schema field builders**: Should we provide fluent builders for schema fields?
   - **Resolution**: Yes, schema field support in Dataset entity

## References

- [Python SDK V2 Implementation](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion/src/datahub/sdk)
- [Existing Java SDK](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/datahub-client)
- [DataHub Metadata Model](https://github.com/datahub-project/datahub/tree/master/metadata-models)

## Quick Links for Reviewers

**Start Here:**

1. `metadata-integration/java/docs/sdk-v2/getting-started.md` - Quick start guide
2. `metadata-integration/java/docs/sdk-v2/design-principles.md` - Architecture overview

**Core Implementation:** 3. `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/Entity.java` (490 lines) - Base entity class 4. `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/operations/EntityClient.java` (570 lines) - CRUD operations 5. `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/DataHubClientV2.java` (266 lines) - Main client

**Sample Entities:** 6. `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/Dataset.java` (564 lines) - Reference implementation 7. `metadata-integration/java/datahub-client/src/main/java/datahub/client/v2/entity/HasTags.java` (145 lines) - CRTP mixin example

**Examples:** 8. `metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DatasetFullExample.java` - Complete workflow 9. `metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartLineageExample.java` - Lineage relationships

**Tests:** 10. `metadata-integration/java/datahub-client/src/test/java/datahub/client/v2/entity/DatasetTest.java` (37 unit tests) 11. `metadata-integration/java/datahub-client/src/test/java/datahub/client/v2/integration/DatasetIntegrationTest.java` - End-to-end validation

---

**Document Status**: Design document reflecting implemented architecture (includes AspectCache refactoring)
**Author**: DataHub OSS Team
**Last Updated**: 2025-01-06
