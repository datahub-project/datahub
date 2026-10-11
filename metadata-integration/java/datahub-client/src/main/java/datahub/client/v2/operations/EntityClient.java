package datahub.client.v2.operations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.config.DataHubClientConfigV2;
import datahub.client.v2.config.ServerConfig;
import datahub.client.v2.entity.Entity;
import datahub.client.v2.exceptions.VersionConflictInfo;
import datahub.client.v2.patch.PatchTransformer;
import datahub.client.v2.patch.TransformResult;
import datahub.client.v2.patch.VersionAwarePatchTransformer;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Client for entity CRUD operations in DataHub.
 *
 * <p>Provides create, read, update, delete, and patch operations for DataHub entities.
 *
 * <p>This class is not meant to be instantiated directly. Use {@link
 * datahub.client.v2.DataHubClientV2#entities()} to obtain an instance.
 */
@Slf4j
public class EntityClient {
  private final RestEmitter emitter;
  private final DataHubClientConfigV2 config;
  private final ObjectMapper objectMapper;
  private final DataHubClientV2 client;
  private final PatchTransformer patchTransformer;

  /**
   * Package-private constructor. Use DataHubClientV2.entities() to obtain an instance.
   *
   * @param emitter the REST emitter for sending metadata
   * @param config the client configuration
   * @param client the parent DataHubClientV2 instance (for server config access)
   */
  public EntityClient(
      @Nonnull RestEmitter emitter,
      @Nonnull DataHubClientConfigV2 config,
      @Nonnull DataHubClientV2 client) {
    this.emitter = emitter;
    this.config = config;
    this.objectMapper = new ObjectMapper();
    this.client = client;
    this.patchTransformer = new VersionAwarePatchTransformer(this);
  }

  /**
   * Upserts an entity (creates if new, updates if exists).
   *
   * <p>This is the primary method for writing entities to DataHub. It:
   *
   * <ul>
   *   <li>Sets the operation mode on the entity from the client config
   *   <li>Emits cached aspects as full upserts if present
   *   <li>Emits all pending patches (accumulated and legacy) if present
   * </ul>
   *
   * @param entity the entity to upsert
   * @param <T> the entity type
   * @throws IOException if emission fails
   * @throws ExecutionException if emission future fails
   * @throws InterruptedException if waiting for emission is interrupted
   */
  public <T extends Entity> void upsert(@Nonnull T entity)
      throws IOException, ExecutionException, InterruptedException {
    log.info("Upserting entity: {}", entity);

    // Bind entity to this client (for lazy loading)
    entity.bindToClient(this, config.getMode());

    List<Future<MetadataWriteResponse>> futures = new ArrayList<>();

    // Strategy: Emit in order of precedence:
    // 1. Cached aspects (from builder) - full aspects for initial creation
    // 2. Pending MCPs (from set*() methods) - full aspect replacements
    // 3. Patches (from add*/remove* methods) - incremental changes

    // Step 1: Emit cached aspects if present
    List<MetadataChangeProposalWrapper> mcps = entity.toMCPs();
    if (!mcps.isEmpty()) {
      log.debug("Entity has {} cached aspects, emitting as upserts", mcps.size());
      for (MetadataChangeProposalWrapper mcp : mcps) {
        log.debug("Emitting MCP for aspect: {}", mcp.getAspect().getClass().getSimpleName());
        futures.add(emitter.emit(mcp));
      }
    }

    // Step 2: Emit pending full aspect MCPs (from set*() methods)
    if (entity.hasPendingMCPs()) {
      List<MetadataChangeProposalWrapper> pendingMCPs = entity.getPendingMCPs();
      log.debug("Entity has {} pending full aspect MCPs, emitting as upserts", pendingMCPs.size());

      for (MetadataChangeProposalWrapper mcp : pendingMCPs) {
        log.debug(
            "Emitting full aspect MCP for aspect: {}", mcp.getAspect().getClass().getSimpleName());
        futures.add(emitter.emit(mcp));
      }

      // Clear after emission
      entity.clearPendingMCPs();
    }

    // Step 3: Wait for all full aspect writes to complete before emitting patches
    // This ensures patches apply to complete aspects and eliminates write-write races
    if (!futures.isEmpty()) {
      log.debug(
          "Waiting for {} full aspect writes to complete before emitting patches", futures.size());

      for (int i = 0; i < futures.size(); i++) {
        Future<MetadataWriteResponse> future = futures.get(i);
        MetadataWriteResponse response = future.get();

        if (!response.isSuccess()) {
          String errorMsg =
              String.format(
                  "Failed to emit full aspect write %d/%d: %s",
                  i + 1, futures.size(), response.getResponseContent());
          log.error(errorMsg);
          throw new IOException(errorMsg);
        }
      }

      log.debug("All {} full aspect writes completed successfully", futures.size());
    }

    // Step 4: Emit all pending patches (both accumulated and legacy)
    // Note: getPendingPatches() now returns both accumulated and legacy patches
    log.error("============ EntityClient.upsert: checking hasPendingPatches() ============");
    boolean hasPatches = entity.hasPendingPatches();
    log.error("hasPendingPatches() returned: {}", hasPatches);

    if (hasPatches) {
      List<com.linkedin.mxe.MetadataChangeProposal> allPatches = entity.getPendingPatches();
      log.error("Entity has {} pending patches before transformation", allPatches.size());
      for (com.linkedin.mxe.MetadataChangeProposal patch : allPatches) {
        log.error("Patch aspect name: {}", patch.getAspectName());
      }

      // Apply version-aware transformation to patches
      ServerConfig serverConfig = client.getServerConfig();
      log.error("Server config: {}", serverConfig);
      List<TransformResult> transformResults;
      try {
        log.error("CALLING patchTransformer.transform()");
        transformResults = patchTransformer.transform(allPatches, serverConfig);
        log.error(
            "Transformed {} patches to {} TransformResults",
            allPatches.size(),
            transformResults.size());
      } catch (Exception e) {
        log.error("Failed to transform patches", e);
        throw new IOException("Failed to transform patches", e);
      }

      // Emit transformed MCPs with retry logic for version conflicts
      for (TransformResult result : transformResults) {
        log.error("Emitting transformed MCP for aspect: {}", result.getMcp().getAspectName());
        try {
          emitWithRetry(result);
        } catch (IOException e) {
          log.error("Failed to emit MCP after retries: {}", e.getMessage());
          throw e;
        }
      }

      // Clear both accumulated builders and pending patches after emission
      entity.clearPatchBuilders();
      entity.clearPendingPatches();
    } else {
      log.error("NO pending patches - skipping transformation");
    }

    log.info("Successfully upserted entity: {}", entity.getUrn());

    // Clear dirty flag now that all mutations have been persisted
    entity.clearDirty();
  }

  /**
   * Retrieves a single aspect from DataHub for the given URN with system metadata.
   *
   * @param urn the URN of the entity
   * @param aspectClass the class of the aspect to retrieve
   * @param <T> the aspect type
   * @return wrapper containing the aspect, system metadata, and version; or non-existent wrapper if
   *     not found
   * @throws IOException if the request fails
   * @throws ExecutionException if the future fails
   * @throws InterruptedException if waiting is interrupted
   */
  @Nonnull
  public <T extends RecordTemplate> AspectWithMetadata<T> getAspect(
      @Nonnull com.linkedin.common.urn.Urn urn, @Nonnull Class<T> aspectClass)
      throws IOException, ExecutionException, InterruptedException {
    log.debug("Fetching aspect {} for entity {}", aspectClass.getSimpleName(), urn);

    String aspectName = getAspectName(aspectClass);
    String encodedUrn = encodeUrn(urn.toString());
    // Include systemMetadata=true to get version information for optimistic locking
    String url =
        String.format(
            "%s/entitiesV2/%s?aspects=List(%s)&systemMetadata=true",
            emitter.getConfig().getServer(), encodedUrn, aspectName);

    log.debug("GET request to: {}", url);
    MetadataWriteResponse response = emitter.get(url).get();

    if (!response.isSuccess()) {
      log.error(
          "Failed to fetch aspect {} for entity {}: {}",
          aspectName,
          urn,
          response.getResponseContent());
      return AspectWithMetadata.nonExistent();
    }

    try {
      String responseContent = response.getResponseContent();
      log.debug("Response content: {}", responseContent);
      JsonNode root = objectMapper.readTree(responseContent);
      log.debug("Parsed JSON root: {}", root);

      JsonNode aspectsNode = root.get("aspects");
      log.debug("Aspects node: {}", aspectsNode);

      if (aspectsNode == null || !aspectsNode.has(aspectName)) {
        log.debug("Aspect {} not found for entity {}", aspectName, urn);
        return AspectWithMetadata.nonExistent();
      }

      JsonNode aspectNode = aspectsNode.get(aspectName);
      log.debug("Aspect node for {}: {}", aspectName, aspectNode);

      JsonNode valueNode = aspectNode.get("value");
      log.debug("Value node: {}", valueNode);

      if (valueNode == null) {
        log.debug("Aspect {} has no value for entity {}", aspectName, urn);
        return AspectWithMetadata.nonExistent();
      }

      // Parse aspect value
      String aspectJson = objectMapper.writeValueAsString(valueNode);
      com.linkedin.data.DataMap dataMap = com.datahub.util.RecordUtils.toDataMap(aspectJson);
      T aspect = com.datahub.util.RecordUtils.toRecordTemplate(aspectClass, dataMap);

      // Parse system metadata if present
      com.linkedin.mxe.SystemMetadata systemMetadata = null;
      JsonNode systemMetadataNode = aspectNode.get("systemMetadata");
      if (systemMetadataNode != null) {
        String systemMetadataJson = objectMapper.writeValueAsString(systemMetadataNode);
        com.linkedin.data.DataMap systemMetadataDataMap =
            com.datahub.util.RecordUtils.toDataMap(systemMetadataJson);
        systemMetadata =
            com.datahub.util.RecordUtils.toRecordTemplate(
                com.linkedin.mxe.SystemMetadata.class, systemMetadataDataMap);
        log.debug("Parsed SystemMetadata with version info");
      }

      log.info("Successfully fetched aspect {} for entity {} with version info", aspectName, urn);
      return AspectWithMetadata.from(aspect, systemMetadata);

    } catch (Exception e) {
      log.error("Failed to parse aspect response for {}: {}", urn, e.getMessage(), e);
      throw new datahub.client.v2.exceptions.AspectParseException(urn, aspectName, e);
    }
  }

  /**
   * Fetches multiple aspects for an entity in a single API call.
   *
   * @param urn the entity URN
   * @param aspectNames list of aspect names to fetch
   * @param aspectClassMap map from aspect name to aspect class
   * @return map of aspect name to aspect instance
   * @throws IOException if network error occurs
   * @throws ExecutionException if server returns error
   * @throws InterruptedException if operation is interrupted
   */
  @Nonnull
  private java.util.Map<String, RecordTemplate> fetchAspects(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull java.util.List<String> aspectNames,
      @Nonnull java.util.Map<String, Class<? extends RecordTemplate>> aspectClassMap)
      throws IOException, ExecutionException, InterruptedException {

    java.util.Map<String, RecordTemplate> result = new java.util.HashMap<>();

    if (aspectNames.isEmpty()) {
      return result;
    }

    log.debug("Fetching {} aspects for entity {} in single request", aspectNames.size(), urn);

    // Build URL with all aspects: ?aspects=List(aspect1,aspect2,aspect3)
    String encodedUrn = encodeUrn(urn.toString());
    String aspectsList = String.join(",", aspectNames);
    String url =
        String.format(
            "%s/entitiesV2/%s?aspects=List(%s)",
            emitter.getConfig().getServer(), encodedUrn, aspectsList);

    log.debug("GET request to: {}", url);
    MetadataWriteResponse response = emitter.get(url).get();

    if (!response.isSuccess()) {
      log.warn("Failed to fetch aspects for entity {}: {}", urn, response.getResponseContent());
      return result;
    }

    try {
      String responseContent = response.getResponseContent();
      log.debug("Response content: {}", responseContent);
      JsonNode root = objectMapper.readTree(responseContent);

      JsonNode aspectsNode = root.get("aspects");
      if (aspectsNode == null) {
        log.debug("No aspects node in response for entity {}", urn);
        return result;
      }

      // Parse each aspect from the response
      for (String aspectName : aspectNames) {
        if (!aspectsNode.has(aspectName)) {
          log.debug("Aspect {} not in response for entity {}", aspectName, urn);
          continue;
        }

        JsonNode aspectNode = aspectsNode.get(aspectName);
        JsonNode valueNode = aspectNode.get("value");

        if (valueNode == null) {
          log.debug("Aspect {} has no value for entity {}", aspectName, urn);
          continue;
        }

        Class<? extends RecordTemplate> aspectClass = aspectClassMap.get(aspectName);
        if (aspectClass == null) {
          log.warn("No aspect class found for aspect name {}", aspectName);
          continue;
        }

        try {
          // Convert JsonNode to JSON string, then to DataMap using RecordUtils codec
          String aspectJson = objectMapper.writeValueAsString(valueNode);
          com.linkedin.data.DataMap dataMap = com.datahub.util.RecordUtils.toDataMap(aspectJson);
          RecordTemplate aspect =
              com.datahub.util.RecordUtils.toRecordTemplate(aspectClass, dataMap);

          result.put(aspectName, aspect);
          log.debug("Successfully parsed aspect {} for entity {}", aspectName, urn);

        } catch (Exception e) {
          log.warn("Failed to parse aspect {} for entity {}: {}", aspectName, urn, e.getMessage());
        }
      }

      log.info("Successfully fetched {} aspects for entity {}", result.size(), urn);
      return result;

    } catch (Exception e) {
      log.error("Failed to parse aspects response for {}: {}", urn, e.getMessage(), e);
      return result;
    }
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
  private String getAspectName(@Nonnull Class<? extends RecordTemplate> aspectClass) {
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
   * URL-encodes a URN string.
   *
   * @param urn the URN to encode
   * @return the encoded URN
   * @throws UnsupportedEncodingException if encoding fails
   */
  @Nonnull
  private String encodeUrn(@Nonnull String urn) throws UnsupportedEncodingException {
    return URLEncoder.encode(urn, StandardCharsets.UTF_8.toString());
  }

  /**
   * Retrieves an entity with its default aspects.
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param <T> the entity type
   * @return the entity with aspects loaded
   * @throws IOException if network error occurs
   * @throws ExecutionException if server returns error
   * @throws InterruptedException if operation is interrupted
   */
  @Nonnull
  public <T extends Entity> T get(@Nonnull String urn, @Nonnull Class<T> entityClass)
      throws IOException, ExecutionException, InterruptedException {
    log.debug("Fetching entity {} with default aspects", urn);

    try {
      // Create URN object
      com.linkedin.common.urn.Urn urnObj = com.linkedin.common.urn.Urn.createFromString(urn);

      // Create entity instance to get default aspects
      T entity = createEntityInstance(urnObj, entityClass);

      // Get default aspects and fetch with them
      List<Class<? extends RecordTemplate>> defaultAspects = entity.getDefaultAspects();
      return get(urn, entityClass, defaultAspects);

    } catch (Exception e) {
      log.error("Failed to fetch entity {}: {}", urn, e.getMessage(), e);
      throw new IOException("Failed to fetch entity", e);
    }
  }

  /**
   * Retrieves an entity with specified aspects.
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param aspects list of aspect classes to fetch
   * @param <T> the entity type
   * @return the entity with specified aspects loaded
   * @throws IOException if network error occurs
   * @throws ExecutionException if server returns error
   * @throws InterruptedException if operation is interrupted
   */
  @Nonnull
  public <T extends Entity> T get(
      @Nonnull String urn,
      @Nonnull Class<T> entityClass,
      @Nonnull List<Class<? extends RecordTemplate>> aspects)
      throws IOException, ExecutionException, InterruptedException {
    log.debug("Fetching entity {} with {} specified aspects", urn, aspects.size());

    try {
      // Create URN object
      com.linkedin.common.urn.Urn urnObj = com.linkedin.common.urn.Urn.createFromString(urn);

      // Build aspect names list for single batch fetch
      java.util.List<String> aspectNames = new java.util.ArrayList<>();
      java.util.Map<String, Class<? extends RecordTemplate>> aspectClassMap =
          new java.util.HashMap<>();

      for (Class<? extends RecordTemplate> aspectClass : aspects) {
        String aspectName = getAspectName(aspectClass);
        aspectNames.add(aspectName);
        aspectClassMap.put(aspectName, aspectClass);
      }

      // Fetch all aspects in a single API call
      java.util.Map<String, RecordTemplate> aspectCache =
          fetchAspects(urnObj, aspectNames, aspectClassMap);

      // Create entity with loaded aspects using the constructor that takes aspects
      T entityWithAspects = createEntityInstance(urnObj, entityClass, aspectCache);

      // Bind entity to this client
      entityWithAspects.bindToClient(this, config.getMode());

      log.info("Successfully fetched entity {} with {} aspects", urn, aspectCache.size());
      return entityWithAspects;

    } catch (Exception e) {
      log.error("Failed to fetch entity {}: {}", urn, e.getMessage(), e);
      throw new IOException("Failed to fetch entity", e);
    }
  }

  /**
   * Creates an entity instance using reflection. Tries to find a constructor that takes just a URN.
   *
   * @param urn the URN object
   * @param entityClass the entity class
   * @param <T> the entity type
   * @return the entity instance
   * @throws Exception if instantiation fails
   */
  @Nonnull
  private <T extends Entity> T createEntityInstance(
      @Nonnull com.linkedin.common.urn.Urn urn, @Nonnull Class<T> entityClass) throws Exception {
    try {
      // Look for constructor that takes specific URN type (e.g., DatasetUrn)
      // or generic Urn type
      java.lang.reflect.Constructor<?>[] constructors = entityClass.getDeclaredConstructors();
      for (java.lang.reflect.Constructor<?> constructor : constructors) {
        Class<?>[] paramTypes = constructor.getParameterTypes();
        // Check if constructor takes a single parameter that is assignable from our URN
        if (paramTypes.length == 1
            && com.linkedin.common.urn.Urn.class.isAssignableFrom(paramTypes[0])
            && paramTypes[0].isInstance(urn)) {
          constructor.setAccessible(true);
          @SuppressWarnings("unchecked")
          T entity = (T) constructor.newInstance(urn);
          return entity;
        }
      }

      throw new IllegalArgumentException(
          "No suitable constructor found for entity class: "
              + entityClass.getName()
              + " with URN type: "
              + urn.getClass().getName());
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate entity class: " + entityClass.getName(), e);
    }
  }

  /**
   * Creates an entity instance with pre-loaded aspects using reflection. Tries to find a
   * constructor that takes URN and aspect map.
   *
   * @param urn the URN object
   * @param entityClass the entity class
   * @param aspects the aspects to load into the entity
   * @param <T> the entity type
   * @return the entity instance
   * @throws Exception if instantiation fails
   */
  @Nonnull
  private <T extends Entity> T createEntityInstance(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Class<T> entityClass,
      @Nonnull java.util.Map<String, RecordTemplate> aspects)
      throws Exception {
    try {
      // Look for constructor that takes URN and Map
      java.lang.reflect.Constructor<?>[] constructors = entityClass.getDeclaredConstructors();
      for (java.lang.reflect.Constructor<?> constructor : constructors) {
        Class<?>[] paramTypes = constructor.getParameterTypes();
        if (paramTypes.length == 2
            && com.linkedin.common.urn.Urn.class.isAssignableFrom(paramTypes[0])
            && paramTypes[0].isInstance(urn)
            && paramTypes[1].equals(java.util.Map.class)) {
          constructor.setAccessible(true);
          @SuppressWarnings("unchecked")
          T entity = (T) constructor.newInstance(urn, aspects);
          return entity;
        }
      }

      throw new IllegalArgumentException(
          "No suitable constructor found for entity class: "
              + entityClass.getName()
              + " with URN type: "
              + urn.getClass().getName());
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate entity class: " + entityClass.getName(), e);
    }
  }

  /**
   * Emits an MCP with retry logic for version conflicts.
   *
   * <p>If the MCP has a retry function and fails with a version conflict, this method will retry up
   * to 3 times with exponential backoff (100ms, 200ms, 400ms).
   *
   * @param result the TransformResult containing the MCP and optional retry function
   * @throws IOException if emission fails after all retries
   * @throws ExecutionException if emission future fails
   * @throws InterruptedException if waiting is interrupted
   */
  private void emitWithRetry(@Nonnull TransformResult result)
      throws IOException, ExecutionException, InterruptedException {
    com.linkedin.mxe.MetadataChangeProposal mcp = result.getMcp();
    int maxRetries = 3;
    long[] backoffDelays = {100L, 200L, 400L}; // Exponential backoff in milliseconds

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      Future<MetadataWriteResponse> future = emitter.emit(mcp, null);
      MetadataWriteResponse response = future.get();

      if (response.isSuccess()) {
        log.debug("Successfully emitted MCP for aspect: {}", mcp.getAspectName());
        return;
      }

      // Check if this is a version conflict
      VersionConflictInfo conflict = parseVersionConflict(response, mcp);

      if (conflict != null && result.hasRetryFunction() && attempt < maxRetries) {
        // Version conflict detected and we have a retry function - retry with fresh read
        log.warn(
            "Version conflict on aspect {}: expected version {}, actual version {}. Attempt {}/{}. Retrying after {}ms...",
            mcp.getAspectName(),
            conflict.getExpectedVersion(),
            conflict.getActualVersion(),
            attempt + 1,
            maxRetries,
            backoffDelays[attempt]);

        // Wait before retrying
        Thread.sleep(backoffDelays[attempt]);

        // Call retry function to get fresh MCP with updated version
        try {
          mcp = result.getRetryFunction().apply(conflict);
          log.debug("Retry function returned fresh MCP with updated version");
        } catch (Exception e) {
          log.error("Retry function failed: {}", e.getMessage(), e);
          throw new IOException("Retry function failed", e);
        }
      } else {
        // Not a version conflict, or no retry function, or out of retries - fail
        String errorMsg =
            String.format(
                "Failed to emit MCP for aspect %s (attempt %d/%d): %s",
                mcp.getAspectName(), attempt + 1, maxRetries + 1, response.getResponseContent());
        log.error(errorMsg);
        throw new IOException(errorMsg);
      }
    }

    // Should never reach here, but just in case
    throw new IOException(
        "Failed to emit MCP for aspect "
            + mcp.getAspectName()
            + " after "
            + maxRetries
            + " retries");
  }

  /**
   * Parses a MetadataWriteResponse to detect version conflicts.
   *
   * <p>Version conflicts are indicated by:
   *
   * <ul>
   *   <li>HTTP 422 status code (Unprocessable Entity)
   *   <li>Error message containing "Expected version X, actual version Y" pattern
   * </ul>
   *
   * @param response the response to parse
   * @param mcp the MCP that was attempted (for context)
   * @return VersionConflictInfo if version conflict detected, null otherwise
   */
  @Nullable
  private VersionConflictInfo parseVersionConflict(
      @Nonnull MetadataWriteResponse response,
      @Nonnull com.linkedin.mxe.MetadataChangeProposal mcp) {
    // Check for 422 status code (Unprocessable Entity)
    // Extract status code from underlying SimpleHttpResponse
    Object underlyingResponse = response.getUnderlyingResponse();
    if (!(underlyingResponse
        instanceof org.apache.hc.client5.http.async.methods.SimpleHttpResponse)) {
      log.debug("Underlying response is not SimpleHttpResponse, skipping version conflict check");
      return null;
    }

    org.apache.hc.client5.http.async.methods.SimpleHttpResponse httpResponse =
        (org.apache.hc.client5.http.async.methods.SimpleHttpResponse) underlyingResponse;
    if (httpResponse.getCode() != 422) {
      return null;
    }

    String errorMessage = response.getResponseContent();
    if (errorMessage == null) {
      return null;
    }

    // Try to parse version conflict pattern: "Expected version X, actual version Y"
    // Pattern variations to handle:
    // - "Expected version 3, actual version 4"
    // - "expected version 3, actual version 4"
    // - "Expected version: 3, actual version: 4"
    java.util.regex.Pattern pattern =
        java.util.regex.Pattern.compile(
            "[Ee]xpected version:?\\s*(\\S+).*[Aa]ctual version:?\\s*(\\S+)",
            java.util.regex.Pattern.DOTALL);
    java.util.regex.Matcher matcher = pattern.matcher(errorMessage);

    if (matcher.find()) {
      String expectedVersion =
          matcher.group(1).replaceAll("[,;]$", ""); // Remove trailing comma/semicolon
      String actualVersion = matcher.group(2).replaceAll("[,;]$", "");

      log.debug(
          "Parsed version conflict: expected={}, actual={}, errorMessage={}",
          expectedVersion,
          actualVersion,
          errorMessage);

      return new VersionConflictInfo(expectedVersion, actualVersion, mcp, errorMessage);
    }

    // Not a version conflict pattern
    return null;
  }

  /**
   * Deletes an entity from DataHub.
   *
   * <p><b>Not yet implemented.</b> This requires integration with DataHub's REST API for deleting
   * entities.
   *
   * @param urn the URN of the entity to delete
   * @throws UnsupportedOperationException always (not yet implemented)
   */
  public void delete(@Nonnull String urn) {
    throw new UnsupportedOperationException(
        "Delete operation not yet implemented. "
            + "This requires REST client for deleting entities from DataHub.");
  }

  /**
   * Checks if an entity exists in DataHub.
   *
   * <p><b>Not yet implemented.</b> This requires integration with DataHub's REST API.
   *
   * @param urn the URN of the entity to check
   * @return true if entity exists
   * @throws UnsupportedOperationException always (not yet implemented)
   */
  public boolean exists(@Nonnull String urn) {
    throw new UnsupportedOperationException(
        "Exists check not yet implemented. " + "This requires REST client integration.");
  }
}
