package datahub.client.v2.entity;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.config.DataHubClientConfigV2;
import datahub.client.v2.operations.EntityClient;
import datahub.event.MetadataChangeProposalWrapper;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Base class for all DataHub entities in SDK V2.
 *
 * <p>This class provides the core functionality for working with DataHub entities, including:
 *
 * <ul>
 *   <li>Aspect management with lazy loading from server
 *   <li>Patch-based updates that accumulate until save()
 *   <li>Mode-aware operations (SDK vs INGESTION mode)
 *   <li>Conversion to Metadata Change Proposals
 * </ul>
 *
 * <p>Entities can be created in three ways:
 *
 * <ol>
 *   <li>From scratch using entity builders (for creating new entities)
 *   <li>Loaded from DataHub server (for updating existing entities)
 *   <li>As references with lazy-loaded aspects
 * </ol>
 *
 * @see Dataset
 * @see Chart
 */
@Slf4j
public abstract class Entity {

  /** Default cache TTL in milliseconds (60 seconds). */
  public static final long DEFAULT_CACHE_TTL_MS = 60000;

  @Getter @Nonnull protected final com.linkedin.common.urn.Urn urn;

  /** Unified cache with read-your-own-writes semantics and dirty tracking. */
  @Nonnull protected final WriteTrackingAspectCache cache;

  /** Pending patch MCPs by aspect name (for incremental updates). */
  @Nonnull protected final Map<String, List<MetadataChangeProposal>> pendingPatches;

  /** Pending full aspect MCPs (from set*() methods) accumulated until save(). */
  @Nonnull protected final List<MetadataChangeProposalWrapper> pendingMCPs;

  /** Accumulated patch builders by aspect name (for combining operations). */
  @Nonnull
  protected final Map<
          String, com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<?>>
      patchBuilders;

  /** Reference to entity client for lazy loading (null if not bound). */
  @Nullable protected EntityClient client;

  /** Operation mode for this entity. */
  @Nullable protected DataHubClientConfigV2.OperationMode mode;

  /**
   * Flag indicating whether this entity has pending mutations.
   *
   * <p>When true, read operations will throw {@link
   * datahub.client.v2.exceptions.PendingMutationsException} to prevent reading potentially stale
   * data. The flag is cleared after successful save via {@link EntityClient#upsert}.
   */
  protected boolean dirty = false;

  /**
   * Flag indicating whether this entity is read-only (immutable).
   *
   * <p>When true, mutation operations will throw {@link
   * datahub.client.v2.exceptions.ReadOnlyEntityException}. Entities fetched from the server are
   * read-only by default. Use {@link #mutable()} to obtain a writable copy.
   *
   * <p>This design enforces immutability for fetched entities, preventing accidental mutations and
   * making it explicit when you want to modify server data.
   */
  protected boolean readOnly = false;

  /**
   * Constructs a new entity from scratch (not loaded from server).
   *
   * @param urn the unique identifier for this entity
   */
  protected Entity(@Nonnull com.linkedin.common.urn.Urn urn) {
    this(urn, DEFAULT_CACHE_TTL_MS);
  }

  /**
   * Constructs a new entity from scratch with custom cache TTL.
   *
   * @param urn the unique identifier for this entity
   * @param cacheTtlMs cache TTL in milliseconds
   */
  protected Entity(@Nonnull com.linkedin.common.urn.Urn urn, long cacheTtlMs) {
    this.urn = urn;
    this.cache = new WriteTrackingAspectCache(cacheTtlMs);
    this.pendingPatches = new HashMap<>();
    this.pendingMCPs = new ArrayList<>();
    this.patchBuilders = new HashMap<>();
    this.client = null;
    this.mode = null; // Will be set when saved through client
  }

  /**
   * Constructs an entity loaded from server with existing aspects.
   *
   * @param urn the unique identifier for this entity
   * @param serverAspects the current aspects of this entity from the server
   */
  protected Entity(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, RecordTemplate> serverAspects) {
    this(urn, serverAspects, DEFAULT_CACHE_TTL_MS);
  }

  /**
   * Constructs an entity loaded from server with existing aspects and custom cache TTL.
   *
   * @param urn the unique identifier for this entity
   * @param serverAspects the current aspects of this entity from the server
   * @param cacheTtlMs cache TTL in milliseconds
   */
  protected Entity(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, RecordTemplate> serverAspects,
      long cacheTtlMs) {
    this.urn = urn;
    this.cache = new WriteTrackingAspectCache(cacheTtlMs);
    // Load server aspects into cache (not dirty, from server)
    for (Map.Entry<String, RecordTemplate> entry : serverAspects.entrySet()) {
      cache.put(entry.getKey(), entry.getValue(), AspectSource.SERVER, false);
    }
    this.pendingPatches = new HashMap<>();
    this.pendingMCPs = new ArrayList<>();
    this.patchBuilders = new HashMap<>();
    this.client = null;
    this.mode = null;
    this.readOnly = true; // Entities fetched from server are read-only by default
    log.debug("Entity {} loaded from server as read-only", urn);
  }

  /**
   * Wide constructor for creating entity copies (used by subclass copy constructors for
   * .mutable()).
   *
   * <p>This constructor allows explicit control over which fields are shared vs freshly allocated
   * when creating a mutable copy of an entity. Subclasses use this in their copy constructors.
   *
   * @param urn the unique identifier for this entity (typically shared)
   * @param cache the aspect cache (typically shared between original and mutable copy)
   * @param client reference to entity client for lazy loading (typically shared)
   * @param mode operation mode for this entity (typically shared)
   * @param pendingPatches pending patch MCPs by aspect name (typically fresh empty map)
   * @param pendingMCPs pending full aspect MCPs (typically fresh empty list)
   * @param patchBuilders accumulated patch builders by aspect name (typically fresh empty map)
   * @param dirty whether this entity has pending mutations (typically false for new mutable copy)
   * @param readOnly whether this entity is immutable (typically false for mutable copy)
   */
  protected Entity(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull WriteTrackingAspectCache cache,
      @Nullable EntityClient client,
      @Nullable DataHubClientConfigV2.OperationMode mode,
      @Nonnull Map<String, List<MetadataChangeProposal>> pendingPatches,
      @Nonnull List<MetadataChangeProposalWrapper> pendingMCPs,
      @Nonnull
          Map<String, com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<?>>
              patchBuilders,
      boolean dirty,
      boolean readOnly) {
    this.urn = urn;
    this.cache = cache;
    this.client = client;
    this.mode = mode;
    this.pendingPatches = pendingPatches;
    this.pendingMCPs = pendingMCPs;
    this.patchBuilders = patchBuilders;
    this.dirty = dirty;
    this.readOnly = readOnly;
  }

  /**
   * Returns the entity type name (e.g., "dataset", "chart", "dashboard").
   *
   * @return the entity type name
   */
  @Nonnull
  public abstract String getEntityType();

  /**
   * Returns the list of aspect classes that should be fetched by default when loading this entity
   * from the server.
   *
   * @return list of aspect classes to fetch
   */
  @Nonnull
  public abstract List<Class<? extends RecordTemplate>> getDefaultAspects();

  /**
   * Returns whether this entity was created from scratch (true) or loaded from server (false).
   *
   * <p>An entity is considered "new" if it has no aspects in cache. When an entity is loaded from
   * the server, its aspects are immediately cached. Pending patches do not affect whether an entity
   * is considered new.
   *
   * @return true if entity is new (created via builder), false if loaded from server
   */
  public boolean isNewEntity() {
    return cache.isEmpty();
  }

  /**
   * Binds entity to an entity client for lazy loading.
   *
   * @param client the entity client to bind
   * @param mode the operation mode
   */
  public void bindToClient(
      @Nonnull EntityClient client, @Nonnull DataHubClientConfigV2.OperationMode mode) {
    if (this.client == null) {
      this.client = client;
    }
    if (this.mode == null) {
      this.mode = mode;
    }
  }

  /**
   * Sets the operation mode for this entity.
   *
   * @param mode the operation mode
   */
  public void setMode(@Nonnull DataHubClientConfigV2.OperationMode mode) {
    this.mode = mode;
  }

  /**
   * Returns whether this entity is in ingestion mode.
   *
   * @return true if in ingestion mode
   */
  public boolean isIngestionMode() {
    return mode == DataHubClientConfigV2.OperationMode.INGESTION;
  }

  /**
   * Returns whether this entity is in SDK mode.
   *
   * @return true if in SDK mode
   */
  public boolean isSdkMode() {
    return mode == DataHubClientConfigV2.OperationMode.SDK || mode == null;
  }

  /**
   * Marks this entity as dirty (has pending mutations).
   *
   * <p>Called automatically by mutation methods. When dirty, read operations will throw {@link
   * datahub.client.v2.exceptions.PendingMutationsException}.
   */
  protected void markDirty() {
    this.dirty = true;
    log.debug("Entity {} marked as dirty (pending mutations)", urn);
  }

  /**
   * Clears the dirty flag, indicating all mutations have been persisted.
   *
   * <p>Called automatically by {@link EntityClient#upsert} after successful save.
   */
  public void clearDirty() {
    this.dirty = false;
    log.debug("Entity {} dirty flag cleared", urn);
  }

  /**
   * Returns whether this entity has pending mutations.
   *
   * @return true if entity has been modified and not yet saved
   */
  public boolean isDirty() {
    return dirty;
  }

  /**
   * Checks that the entity is not dirty before performing a read operation.
   *
   * <p>Throws {@link datahub.client.v2.exceptions.PendingMutationsException} if the entity has
   * pending mutations. This prevents reading potentially stale cached data.
   *
   * @param operation description of the read operation being attempted
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity is dirty
   */
  protected void checkNotDirty(@Nonnull String operation) {
    if (dirty) {
      throw new datahub.client.v2.exceptions.PendingMutationsException(getEntityType(), operation);
    }
  }

  /**
   * Returns whether this entity is read-only (immutable).
   *
   * <p>Read-only entities are fetched from the server and cannot be mutated directly. Use {@link
   * #mutable()} to obtain a writable copy.
   *
   * @return true if entity is read-only
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Returns whether this entity is mutable (can be modified).
   *
   * <p>Entities are mutable if they were created via builders or obtained via {@link #mutable()}.
   * Entities fetched from the server are immutable by default.
   *
   * @return true if entity is mutable (not read-only)
   */
  public boolean isMutable() {
    return !readOnly;
  }

  /**
   * Checks that the entity is not read-only before performing a mutation operation.
   *
   * <p>Throws {@link datahub.client.v2.exceptions.ReadOnlyEntityException} if the entity is
   * read-only. This prevents accidental mutations of fetched entities.
   *
   * @param operation description of the mutation operation being attempted
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  protected void checkNotReadOnly(@Nonnull String operation) {
    if (readOnly) {
      throw new datahub.client.v2.exceptions.ReadOnlyEntityException(getEntityType(), operation);
    }
  }

  /**
   * Creates a mutable copy of this entity for mutation.
   *
   * <p>This method is used to convert a read-only entity (fetched from server) into a mutable
   * entity that can be modified and saved back.
   *
   * <p>The copy shares the aspect cache with the original, so reads will see the same data. However
   * the dirty and readOnly flags are independent, allowing the copy to be mutated while the
   * original remains immutable.
   *
   * <p>If the entity is already mutable, returns itself (idempotent).
   *
   * <p><b>Example usage:</b>
   *
   * <pre>{@code
   * // Fetch entity from server (read-only)
   * Dataset dataset = client.entities().get(urn);
   * dataset.isMutable();  // false
   * dataset.getDescription();  // Works fine
   *
   * // Get mutable copy for updates
   * Dataset mutable = dataset.mutable();
   * mutable.isMutable();  // true
   * mutable.setDescription("Updated");
   * client.entities().upsert(mutable);
   * }</pre>
   *
   * @return a mutable copy of this entity
   * @param <T> the entity type
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public <T extends Entity> T mutable() {
    if (!readOnly) {
      // Already mutable, return self
      return (T) this;
    }

    try {
      // Create a shallow copy using reflection
      T copy = (T) this.getClass().getDeclaredConstructor().newInstance();

      // Copy all fields via reflection
      java.lang.reflect.Field urnField = Entity.class.getDeclaredField("urn");
      urnField.setAccessible(true);
      urnField.set(copy, this.urn);

      java.lang.reflect.Field cacheField = Entity.class.getDeclaredField("cache");
      cacheField.setAccessible(true);
      cacheField.set(copy, this.cache); // Share cache

      java.lang.reflect.Field pendingPatchesField = Entity.class.getDeclaredField("pendingPatches");
      pendingPatchesField.setAccessible(true);
      pendingPatchesField.set(copy, new HashMap<>(this.pendingPatches));

      java.lang.reflect.Field pendingMCPsField = Entity.class.getDeclaredField("pendingMCPs");
      pendingMCPsField.setAccessible(true);
      pendingMCPsField.set(copy, new ArrayList<>(this.pendingMCPs));

      java.lang.reflect.Field patchBuildersField = Entity.class.getDeclaredField("patchBuilders");
      patchBuildersField.setAccessible(true);
      patchBuildersField.set(copy, new HashMap<>(this.patchBuilders));

      java.lang.reflect.Field clientField = Entity.class.getDeclaredField("client");
      clientField.setAccessible(true);
      clientField.set(copy, this.client);

      java.lang.reflect.Field modeField = Entity.class.getDeclaredField("mode");
      modeField.setAccessible(true);
      modeField.set(copy, this.mode);

      java.lang.reflect.Field dirtyField = Entity.class.getDeclaredField("dirty");
      dirtyField.setAccessible(true);
      dirtyField.set(copy, false); // New copy starts clean

      java.lang.reflect.Field readOnlyField = Entity.class.getDeclaredField("readOnly");
      readOnlyField.setAccessible(true);
      readOnlyField.set(copy, false); // New copy is mutable

      log.debug("Created mutable copy of entity {}", urn);
      return copy;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create mutable copy of entity", e);
    }
  }

  /**
   * Gets an aspect from cache only (no server fetch).
   *
   * @param aspectClass the class of the aspect to retrieve
   * @param <T> the aspect type
   * @return the aspect if cached, null otherwise
   */
  @Nullable
  protected <T extends RecordTemplate> T getAspectCached(@Nonnull Class<T> aspectClass) {
    String aspectName = getAspectName(aspectClass);
    return cache.get(aspectName, aspectClass, ReadMode.ALLOW_DIRTY);
  }

  /**
   * Updates an aspect in the cache, marking it as dirty (pending write).
   *
   * <p>This method is used by entity set*() methods to cache modified aspects that need to be
   * written to the server.
   *
   * @param aspectName the name of the aspect
   * @param aspect the aspect data to cache
   */
  protected void updateAspectCached(@Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    cache.put(aspectName, aspect, AspectSource.LOCAL, true);
    markDirty();
  }

  /**
   * Marks an aspect as dirty (modified and needing write-back to server).
   *
   * <p>This is useful when you modify an aspect in-place that was fetched from the server. The
   * aspect needs to be marked dirty so it will be included in the next upsert.
   *
   * @param aspectClass the class of the aspect to mark dirty
   */
  protected void markAspectDirty(@Nonnull Class<? extends RecordTemplate> aspectClass) {
    String aspectName = getAspectName(aspectClass);
    cache.markDirty(aspectName);
    markDirty();
  }

  /**
   * Gets an aspect, fetching from server if needed (lazy loading).
   *
   * <p>If client is bound and aspect is not cached or expired, fetches from server. The AspectCache
   * handles TTL expiration internally.
   *
   * @param aspectClass the class of the aspect to retrieve
   * @param <T> the aspect type
   * @return the aspect if available, null otherwise
   */
  @Nullable
  public <T extends RecordTemplate> T getAspectLazy(@Nonnull Class<T> aspectClass) {
    String aspectName = getAspectName(aspectClass);

    // Try to get from cache (WriteTrackingAspectCache handles TTL expiration internally)
    T cached = cache.get(aspectName, aspectClass, ReadMode.ALLOW_DIRTY);
    if (cached != null) {
      return cached;
    }

    // Prevent lazy loading from server if entity has pending mutations
    // (to avoid mixed state of old server data + new local mutations)
    if (dirty && client != null) {
      log.debug(
          "Skipping lazy-load of aspect {} for entity {} - entity has pending mutations",
          aspectName,
          urn);
      return null;
    }

    // Fetch from server if client is bound
    if (client != null) {
      try {
        datahub.client.v2.operations.AspectWithMetadata<T> aspectWithMetadata =
            client.getAspect(urn, aspectClass);
        T aspect = aspectWithMetadata.getAspect();
        if (aspect != null) {
          cache.put(aspectName, aspect, AspectSource.SERVER, false);
        }
        return aspect;
      } catch (Exception e) {
        log.error(
            "Failed to lazy-load aspect {} for entity {}: {}", aspectName, urn, e.getMessage(), e);
        throw new datahub.client.v2.exceptions.AspectFetchException(urn, aspectName, e);
      }
    }

    return null;
  }

  /**
   * Gets an aspect, creating it with default constructor if it doesn't exist.
   *
   * <p>This does NOT fetch from server - it creates a new empty aspect locally. The newly created
   * aspect is marked as LOCAL-sourced and dirty (needs to be written to server).
   *
   * @param aspectClass the class of the aspect to retrieve or create
   * @param <T> the aspect type
   * @return the existing or newly created aspect
   * @throws RuntimeException if aspect cannot be instantiated
   */
  @Nonnull
  protected <T extends RecordTemplate> T getOrCreateAspect(@Nonnull Class<T> aspectClass) {
    T existing = getAspectCached(aspectClass);
    if (existing != null) {
      return existing;
    }

    // Create new instance using default constructor
    try {
      T newAspect = aspectClass.getDeclaredConstructor().newInstance();
      String aspectName = getAspectName(aspectClass);
      cache.put(aspectName, newAspect, AspectSource.LOCAL, true);
      return newAspect;
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new RuntimeException("Failed to instantiate aspect: " + aspectClass.getName(), e);
    }
  }

  /**
   * Removes an aspect from cache.
   *
   * <p>This method is useful when you need to ensure that patch operations take effect on entities
   * that were fetched from the server. When an aspect is cached, full aspect upserts will override
   * patches. Removing the aspect from cache allows patches to be applied correctly.
   *
   * @param aspectClass the class of the aspect to remove
   * @param <T> the aspect type
   * @return the removed aspect, or null if it wasn't present
   */
  @Nullable
  public <T extends RecordTemplate> T removeAspect(@Nonnull Class<T> aspectClass) {
    String aspectName = getAspectName(aspectClass);
    RecordTemplate removed = cache.remove(aspectName);
    if (removed != null && aspectClass.isInstance(removed)) {
      return aspectClass.cast(removed);
    }
    return null;
  }

  /**
   * Adds a patch MCP to the pending list.
   *
   * @param patch the patch MCP to add
   */
  protected void addPatchMcp(@Nonnull MetadataChangeProposal patch) {
    String aspectName = patch.getAspectName();
    pendingPatches.computeIfAbsent(aspectName, k -> new ArrayList<>()).add(patch);
    markDirty();
  }

  /**
   * Returns all pending patch MCPs, including both accumulated patches and legacy pending patches.
   *
   * <p>This method combines patches from accumulated patch builders and the legacy pending patches
   * list, providing a unified view of all pending operations.
   *
   * @return list of all pending patches
   */
  @Nonnull
  public List<MetadataChangeProposal> getPendingPatches() {
    List<MetadataChangeProposal> allPatches = new ArrayList<>();

    // Add accumulated patches from builders
    allPatches.addAll(buildAccumulatedPatches());

    // Add legacy pending patches for backward compatibility - flatten the map
    for (List<MetadataChangeProposal> patches : pendingPatches.values()) {
      allPatches.addAll(patches);
    }

    return allPatches;
  }

  /**
   * Returns whether this entity has pending patches (including accumulated patches).
   *
   * @return true if there are pending patches or accumulated patch builders
   */
  public boolean hasPendingPatches() {
    return !pendingPatches.isEmpty() || !patchBuilders.isEmpty();
  }

  /** Clears all pending patches and accumulated patch builders. */
  public void clearPendingPatches() {
    pendingPatches.clear();
    patchBuilders.clear();
  }

  /**
   * Adds a full aspect MCP to the pending list (from set*() methods).
   *
   * <p>This method is used when set*() methods need to replace entire aspects rather than apply
   * patches. The MCP will be emitted when the entity is saved.
   *
   * @param mcp the full aspect MCP to add
   */
  protected void addPendingMCP(@Nonnull MetadataChangeProposalWrapper mcp) {
    String aspectName = mcp.getAspectName();

    // Remove any existing pending MCP for this aspect (last one wins)
    pendingMCPs.removeIf(existing -> aspectName.equals(existing.getAspectName()));

    // Clear any accumulated patch builders for this aspect (full replacement supersedes patches)
    patchBuilders.remove(aspectName);

    // Also remove patch MCPs for this aspect from pendingPatches
    pendingPatches.remove(aspectName);

    // Add the new full aspect MCP
    pendingMCPs.add(mcp);

    markDirty();
  }

  /**
   * Returns all pending full aspect MCPs (from set*() methods).
   *
   * @return list of pending full aspect MCPs
   */
  @Nonnull
  public List<MetadataChangeProposalWrapper> getPendingMCPs() {
    return new ArrayList<>(pendingMCPs);
  }

  /**
   * Returns whether this entity has pending full aspect MCPs.
   *
   * @return true if there are pending full aspect MCPs
   */
  public boolean hasPendingMCPs() {
    return !pendingMCPs.isEmpty();
  }

  /** Clears all pending full aspect MCPs. */
  public void clearPendingMCPs() {
    pendingMCPs.clear();
  }

  /**
   * Registers a patch builder for a specific aspect.
   *
   * @param aspectName the aspect name
   * @param builder the patch builder
   */
  protected void registerPatchBuilder(
      @Nonnull String aspectName,
      @Nonnull
          com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<?> builder) {
    patchBuilders.put(aspectName, builder);
    markDirty();
  }

  /**
   * Gets a patch builder for a specific aspect.
   *
   * @param aspectName the aspect name
   * @param builderClass the builder class
   * @param <T> the builder type
   * @return the builder, or null if not registered
   */
  @Nullable
  protected <T extends com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<T>>
      T getPatchBuilder(@Nonnull String aspectName, @Nonnull Class<T> builderClass) {
    com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<?> builder =
        patchBuilders.get(aspectName);
    if (builder != null && builderClass.isInstance(builder)) {
      @SuppressWarnings("unchecked")
      T typedBuilder = (T) builder;
      return typedBuilder;
    }
    return null;
  }

  /**
   * Returns whether this entity has accumulated patch builders.
   *
   * @return true if there are accumulated patch builders
   */
  public boolean hasAccumulatedPatchBuilders() {
    return !patchBuilders.isEmpty();
  }

  /**
   * Builds all accumulated patch builders into MCPs.
   *
   * @return list of MCPs from accumulated builders
   */
  @Nonnull
  public List<MetadataChangeProposal> buildAccumulatedPatches() {
    List<MetadataChangeProposal> patches = new ArrayList<>();
    for (com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder<?> builder :
        patchBuilders.values()) {
      // Check if builder has operations (pathValues is protected, so we check by trying to build)
      try {
        MetadataChangeProposal mcp = builder.build();
        patches.add(mcp);
        log.debug("Built accumulated patch for aspect: {}", mcp.getAspectName());
      } catch (IllegalArgumentException e) {
        // Builder has no operations, skip
        log.debug("Skipping empty patch builder");
      }
    }
    return patches;
  }

  /** Clears all accumulated patch builders. */
  public void clearPatchBuilders() {
    patchBuilders.clear();
  }

  /**
   * Gets the aspect name from an aspect class. Tries the following in order: 1. ASPECT_NAME field
   * (if present) 2. @Aspect.name annotation from the Pegasus schema (correctly handles acronyms
   * like "MLModelProperties" -> "mlModelProperties") 3. Fallback to camelCase version of simple
   * class name (may be incorrect for acronyms)
   *
   * @param aspectClass the aspect class
   * @return the aspect name
   */
  @Nonnull
  protected String getAspectName(@Nonnull Class<? extends RecordTemplate> aspectClass) {
    // Try ASPECT_NAME field first
    try {
      return (String) aspectClass.getField("ASPECT_NAME").get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      // ASPECT_NAME field not found, try schema annotation
    }

    // Try to extract from Pegasus schema annotation (SCHEMA field is private, use getDeclaredField)
    try {
      java.lang.reflect.Field schemaField = aspectClass.getDeclaredField("SCHEMA");
      schemaField.setAccessible(true);
      RecordDataSchema schema = (RecordDataSchema) schemaField.get(null);
      Object aspectAnnotationObj = schema.getProperties().get(AspectAnnotation.ANNOTATION_NAME);
      if (aspectAnnotationObj != null) {
        String aspectName =
            AspectAnnotation.fromSchemaProperty(aspectAnnotationObj, schema.getFullName())
                .getName();
        log.debug(
            "Extracted aspect name from schema for class {}: {}",
            aspectClass.getName(),
            aspectName);
        return aspectName;
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      log.debug(
          "Failed to extract aspect name from schema for class {}: {}",
          aspectClass.getName(),
          e.getMessage());
    }

    // Final fallback to camelCase (may be incorrect for acronyms like MLModelProperties)
    String simpleName = aspectClass.getSimpleName();
    String camelCaseName = simpleName.substring(0, 1).toLowerCase() + simpleName.substring(1);
    log.warn(
        "Aspect class {} does not have ASPECT_NAME field or schema annotation, using camelCase fallback: {}. This may be incorrect for classes with acronyms.",
        aspectClass.getName(),
        camelCaseName);
    return camelCaseName;
  }

  /**
   * Converts this entity to a list of Metadata Change Proposals for emission.
   *
   * <p>Returns only dirty aspects as MCPs (aspects with pending writes). Clean aspects fetched from
   * the server that haven't been modified locally are not included.
   *
   * @return list of MCPs representing this entity's dirty aspects
   */
  @Nonnull
  public List<MetadataChangeProposalWrapper> toMCPs() {
    List<MetadataChangeProposalWrapper> mcps = new ArrayList<>();

    // Only emit dirty aspects (from builder or local modifications)
    // Clean server-fetched aspects are not re-emitted
    log.debug(
        "Entity.toMCPs() called for urn={}, dirty aspects={}",
        urn,
        cache.getDirtyAspects().keySet());
    for (Map.Entry<String, RecordTemplate> entry : cache.getDirtyAspects().entrySet()) {
      log.debug("Creating MCP for dirty aspect: {}, value: {}", entry.getKey(), entry.getValue());
      MetadataChangeProposalWrapper mcp =
          MetadataChangeProposalWrapper.builder()
              .entityType(getEntityType())
              .entityUrn(urn)
              .upsert()
              .aspect(entry.getValue())
              .build();
      mcps.add(mcp);
    }

    log.debug("Generated {} MCPs from dirty aspects", mcps.size());
    return mcps;
  }

  /**
   * Returns all cached aspects of this entity (both clean and dirty).
   *
   * @return map of aspect names to their aspect data
   */
  @Nonnull
  public Map<String, RecordTemplate> getAllAspects() {
    return cache.getAllAspects();
  }

  @Override
  public String toString() {
    return String.format(
        "%s{urn=%s, cachedAspects=%d, pendingMCPs=%d, pendingPatches=%d, mode=%s}",
        getClass().getSimpleName(),
        urn,
        cache.size(),
        pendingMCPs.size(),
        pendingPatches.size(),
        mode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Entity entity = (Entity) o;
    return urn.equals(entity.urn);
  }

  @Override
  public int hashCode() {
    return urn.hashCode();
  }
}
