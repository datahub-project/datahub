package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.EditableDatasetPropertiesPatchBuilder;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import datahub.client.v2.annotations.RequiresMutable;
import datahub.client.v2.operations.EntityClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub Dataset entity with fluent builder API.
 *
 * <p>A Dataset represents a collection of data with a common schema in a data platform (e.g., a
 * table in a database, a topic in Kafka, a file in S3).
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>The SDK supports two operation modes:
 *
 * <ul>
 *   <li>SDK mode (default): User-initiated edits write to editable aspects (e.g.,
 *       editableDatasetProperties)
 *   <li>INGESTION mode: System/pipeline writes write to system aspects (e.g., datasetProperties)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_database.my_schema.my_table")
 *     .env("PROD")
 *     .description("Customer transactions table")
 *     .build();
 *
 * // Add metadata (creates patches)
 * dataset.addTag("pii");
 * dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
 * dataset.addTerm("urn:li:glossaryTerm:CustomerData");
 * dataset.setDomain("urn:li:domain:Marketing");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(dataset);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class Dataset extends Entity
    implements HasTags<Dataset>,
        HasGlossaryTerms<Dataset>,
        HasOwners<Dataset>,
        HasDomains<Dataset>,
        HasSubTypes<Dataset>,
        HasStructuredProperties<Dataset> {

  private static final String ENTITY_TYPE = "dataset";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Constructs a new Dataset entity.
   *
   * @param urn the dataset URN
   */
  private Dataset(@Nonnull DatasetUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new Dataset entity from generic URN.
   *
   * @param urn the dataset URN (generic type)
   */
  protected Dataset(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs a Dataset loaded from server.
   *
   * @param urn the dataset URN
   * @param aspects current aspects from server
   */
  private Dataset(
      @Nonnull DatasetUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs a Dataset loaded from server with generic URN.
   *
   * @param urn the dataset URN (generic type)
   * @param aspects current aspects from server
   */
  protected Dataset(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Copy constructor for creating mutable copy via .mutable().
   *
   * <p>This constructor creates a new Dataset instance that shares immutable fields (urn, cache,
   * client, mode) with the original, but has fresh mutation tracking fields (pendingPatches,
   * pendingMCPs, patchBuilders) and is marked as mutable.
   *
   * @param other the Dataset to copy from
   */
  protected Dataset(@Nonnull Dataset other) {
    super(
        other.urn,
        other.cache,
        other.client,
        other.mode,
        new HashMap<>(),
        new ArrayList<>(),
        new HashMap<>(),
        false,
        false);
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public List<Class<? extends com.linkedin.data.template.RecordTemplate>> getDefaultAspects() {
    return List.of(
        com.linkedin.common.Ownership.class,
        com.linkedin.common.GlobalTags.class,
        com.linkedin.common.GlossaryTerms.class,
        com.linkedin.domain.Domains.class,
        com.linkedin.common.Status.class,
        com.linkedin.common.InstitutionalMemory.class,
        DatasetProperties.class,
        EditableDatasetProperties.class);
  }

  /**
   * Returns a mutable copy of this Dataset entity.
   *
   * <p>If this entity is already mutable, returns this instance (idempotent). If this entity is
   * read-only (fetched from server), creates and returns a new mutable copy that shares the aspect
   * cache and other immutable fields, but has independent mutation tracking.
   *
   * @return this instance if already mutable, or a new mutable copy if read-only
   */
  @Override
  @Nonnull
  public Dataset mutable() {
    if (!readOnly) {
      return this;
    }
    return new Dataset(this);
  }

  /**
   * Returns the dataset URN.
   *
   * @return the dataset URN
   */
  @Nonnull
  public DatasetUrn getDatasetUrn() {
    return (DatasetUrn) urn;
  }

  /**
   * Creates a new builder for Dataset.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<Dataset> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<Dataset> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<Dataset> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<Dataset> interface

  // ==================== Description Operations (Mode-Aware) ====================

  /**
   * Sets the description for this dataset (mode-aware).
   *
   * <p>In SDK mode, writes to editableDatasetProperties. In INGESTION mode, writes to
   * datasetProperties.
   *
   * @param description the description
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    if (isIngestionMode()) {
      return setSystemDescription(description);
    } else {
      return setEditableDescription(description);
    }
  }

  /**
   * Sets the system description (datasetProperties).
   *
   * @param description the description
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setSystemDescription(@Nonnull String description) {
    checkNotReadOnly("set system description");
    DatasetPropertiesPatchBuilder patch =
        new DatasetPropertiesPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added system description patch");
    return this;
  }

  /**
   * Sets the editable description (editableDatasetProperties).
   *
   * @param description the description
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setEditableDescription(@Nonnull String description) {
    checkNotReadOnly("set editable description");
    EditableDatasetPropertiesPatchBuilder patch =
        new EditableDatasetPropertiesPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added editable description patch");
    return this;
  }

  /**
   * Gets the description for this dataset (prefers editable over system).
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    // Prefer editable description (matches Python SDK V2 behavior)
    EditableDatasetProperties editable = getAspectLazy(EditableDatasetProperties.class);
    if (editable != null
        && editable.getDescription() != null
        && !editable.getDescription().trim().isEmpty()) {
      return editable.getDescription();
    }

    // Fall back to system description
    DatasetProperties props = getAspectLazy(DatasetProperties.class);
    return props != null ? props.getDescription() : null;
  }

  // ==================== Display Name Operations (Mode-Aware) ====================

  /**
   * Sets the display name for this dataset (mode-aware).
   *
   * @param name the display name
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setDisplayName(@Nonnull String name) {
    checkNotReadOnly("set display name");
    if (isIngestionMode()) {
      return setSystemDisplayName(name);
    } else {
      return setEditableDisplayName(name);
    }
  }

  /**
   * Sets the system display name (datasetProperties).
   *
   * @param name the display name
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setSystemDisplayName(@Nonnull String name) {
    checkNotReadOnly("set system display name");
    DatasetPropertiesPatchBuilder patch =
        new DatasetPropertiesPatchBuilder().urn(getUrn()).setName(name);
    addPatchMcp(patch.build());
    log.debug("Added system display name patch");
    return this;
  }

  /**
   * Sets the editable display name (editableDatasetProperties).
   *
   * @param name the display name
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setEditableDisplayName(@Nonnull String name) {
    checkNotReadOnly("set editable display name");
    EditableDatasetPropertiesPatchBuilder patch =
        new EditableDatasetPropertiesPatchBuilder().urn(getUrn()).setName(name);
    addPatchMcp(patch.build());
    log.debug("Added editable display name patch");
    return this;
  }

  /**
   * Gets the display name for this dataset (prefers editable over system).
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getDisplayName() {
    // Prefer editable name
    EditableDatasetProperties editable = getAspectLazy(EditableDatasetProperties.class);
    if (editable != null && editable.getName() != null && !editable.getName().trim().isEmpty()) {
      return editable.getName();
    }

    // Fall back to system name
    DatasetProperties props = getAspectLazy(DatasetProperties.class);
    return props != null ? props.getName() : null;
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Adds a custom property to this dataset.
   *
   * @param key the property key
   * @param value the property value
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    // Get or create accumulated patch builder for datasetProperties
    DatasetPropertiesPatchBuilder builder =
        getPatchBuilder("datasetProperties", DatasetPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new DatasetPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("datasetProperties", builder);
    }
    builder.addCustomProperty(key, value);
    log.debug("Added custom property to accumulated patch: {}={}", key, value);
    return this;
  }

  /**
   * Removes a custom property from this dataset.
   *
   * @param key the property key to remove
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset removeCustomProperty(@Nonnull String key) {
    checkNotReadOnly("remove custom property");
    // Get or create accumulated patch builder for datasetProperties
    DatasetPropertiesPatchBuilder builder =
        getPatchBuilder("datasetProperties", DatasetPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new DatasetPropertiesPatchBuilder().urn(getUrn());
      registerPatchBuilder("datasetProperties", builder);
    }
    builder.removeCustomProperty(key);
    log.debug("Added remove custom property to accumulated patch: {}", key);
    return this;
  }

  /**
   * Sets custom properties for this dataset (replaces all).
   *
   * @param properties map of custom properties
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    DatasetPropertiesPatchBuilder patch =
        new DatasetPropertiesPatchBuilder().urn(getUrn()).setCustomProperties(properties);
    addPatchMcp(patch.build());
    log.debug("Added set custom properties patch with {} properties", properties.size());
    return this;
  }

  // ==================== Schema Operations ====================

  /**
   * Sets the schema metadata for this dataset.
   *
   * <p>Note: Schema is typically set in full, not patched, so this caches the aspect directly.
   *
   * @param schema the schema metadata
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setSchema(@Nonnull SchemaMetadata schema) {
    checkNotReadOnly("set schema");
    String aspectName = getAspectName(SchemaMetadata.class);
    cache.put(aspectName, schema, AspectSource.LOCAL, true);
    log.debug(
        "Set schema with {} fields", schema.getFields() != null ? schema.getFields().size() : 0);
    return this;
  }

  /**
   * Sets the schema fields for this dataset.
   *
   * @param fields list of schema fields
   * @return this dataset
   */
  @RequiresMutable
  @Nonnull
  public Dataset setSchemaFields(@Nonnull List<SchemaField> fields) {
    checkNotReadOnly("set schema fields");
    SchemaMetadata schema = new SchemaMetadata();
    schema.setFields(new SchemaFieldArray(fields));
    schema.setPlatform(getDatasetUrn().getPlatformEntity());
    schema.setSchemaName(getDatasetUrn().getDatasetNameEntity());
    schema.setVersion(0L);
    schema.setHash("");
    // Set a default platformSchema (required field)
    // Use a simple MySqlDDL schema as default
    schema.setPlatformSchema(
        SchemaMetadata.PlatformSchema.create(
            new com.linkedin.schema.MySqlDDL().setTableSchema("")));
    return setSchema(schema);
  }

  /**
   * Gets the schema metadata for this dataset (lazy-loaded).
   *
   * @return the schema metadata, or null if not set
   */
  @Nullable
  public SchemaMetadata getSchema() {
    return getAspectLazy(SchemaMetadata.class);
  }

  // ==================== Builder ====================

  /** Builder for creating Dataset entities with a fluent API. */
  public static class Builder {
    private String platform;
    private String name;
    private String env = "PROD";
    private String platformInstance;
    private String description;
    private String displayName;
    private List<SchemaField> schemaFields;
    private Map<String, String> customProperties;

    /**
     * Sets the platform name (required).
     *
     * @param platform the platform name (e.g., "snowflake", "bigquery", "kafka")
     * @return this builder
     */
    @Nonnull
    public Builder platform(@Nonnull String platform) {
      this.platform = platform;
      return this;
    }

    /**
     * Sets the dataset name (required).
     *
     * @param name the dataset name (e.g., "my_table", "my_database.my_schema.my_table")
     * @return this builder
     */
    @Nonnull
    public Builder name(@Nonnull String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the environment. Default is "PROD".
     *
     * @param env the environment (e.g., "PROD", "DEV", "QA")
     * @return this builder
     */
    @Nonnull
    public Builder env(@Nonnull String env) {
      this.env = env;
      return this;
    }

    /**
     * Sets the platform instance.
     *
     * @param platformInstance the platform instance
     * @return this builder
     */
    @Nonnull
    public Builder platformInstance(@Nullable String platformInstance) {
      this.platformInstance = platformInstance;
      return this;
    }

    /**
     * Sets the description.
     *
     * @param description the description
     * @return this builder
     */
    @Nonnull
    public Builder description(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the display name.
     *
     * @param displayName the display name
     * @return this builder
     */
    @Nonnull
    public Builder displayName(@Nullable String displayName) {
      this.displayName = displayName;
      return this;
    }

    /**
     * Sets the schema fields.
     *
     * @param fields list of schema fields
     * @return this builder
     */
    @Nonnull
    public Builder schemaFields(@Nullable List<SchemaField> fields) {
      this.schemaFields = fields;
      return this;
    }

    /**
     * Sets custom properties.
     *
     * @param properties map of custom properties
     * @return this builder
     */
    @Nonnull
    public Builder customProperties(@Nullable Map<String, String> properties) {
      this.customProperties = properties;
      return this;
    }

    /**
     * Sets the URN for this dataset by extracting its components.
     *
     * @param urn the dataset URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull DatasetUrn urn) {
      this.platform = urn.getPlatformEntity().getPlatformNameEntity();
      this.name = urn.getDatasetNameEntity();
      this.env = urn.getOriginEntity().name();
      return this;
    }

    /**
     * Builds the Dataset entity.
     *
     * <p>Note: Properties set in the builder are cached as aspects, not converted to patches. Use
     * the setter methods on the Dataset after building if you want patch-based updates.
     *
     * @return the Dataset entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public Dataset build() {
      if (platform == null || name == null) {
        throw new IllegalArgumentException("platform and name are required");
      }

      try {
        DataPlatformUrn platformUrn = new DataPlatformUrn(platform);
        String datasetKey =
            platformInstance != null
                ? String.format("(%s,%s,%s)", platformUrn, name, env)
                : String.format("(%s,%s,%s)", platformUrn, name, env);

        DatasetUrn urn = DatasetUrn.createFromString("urn:li:dataset:" + datasetKey);
        Dataset dataset = new Dataset(urn);

        // Cache aspects for builder-provided properties
        if (description != null || displayName != null || customProperties != null) {
          DatasetProperties props = new DatasetProperties();
          if (description != null) {
            props.setDescription(description);
          }
          if (displayName != null) {
            props.setName(displayName);
          }
          if (customProperties != null) {
            props.setCustomProperties(new StringMap(customProperties));
          }
          String aspectName = dataset.getAspectName(DatasetProperties.class);
          dataset.cache.put(aspectName, props, AspectSource.LOCAL, true);
        }

        // Cache schema if provided
        if (schemaFields != null && !schemaFields.isEmpty()) {
          dataset.setSchemaFields(schemaFields);
        }

        // Create DataPlatformInstance aspect if platformInstance is provided
        if (platformInstance != null) {
          com.linkedin.common.DataPlatformInstance dataPlatformInstance =
              new com.linkedin.common.DataPlatformInstance();
          try {
            // Set the platform
            DataPlatformUrn platformUrnForInstance = new DataPlatformUrn(platform);
            dataPlatformInstance.setPlatform(platformUrnForInstance);

            // Create instance URN:
            // urn:li:dataPlatformInstance:(urn:li:dataPlatform:PLATFORM,INSTANCE)
            String instanceUrnString =
                String.format(
                    "urn:li:dataPlatformInstance:(urn:li:dataPlatform:%s,%s)",
                    platform, platformInstance);
            com.linkedin.common.urn.Urn instanceUrn =
                com.linkedin.common.urn.Urn.createFromString(instanceUrnString);
            dataPlatformInstance.setInstance(instanceUrn);

            String aspectName =
                dataset.getAspectName(com.linkedin.common.DataPlatformInstance.class);
            dataset.cache.put(aspectName, dataPlatformInstance, AspectSource.LOCAL, true);
          } catch (URISyntaxException e) {
            log.warn("Failed to create DataPlatformInstance URN: {}", e.getMessage());
          }
        }

        return dataset;
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create dataset URN", e);
      }
    }
  }

  // ==================== Patch Transformation (Version Compatibility) ====================

  /**
   * Transforms an editableDatasetProperties patch MCP to a full aspect replacement MCP via
   * read-modify-write.
   *
   * <p>This is used by the VersionAwarePatchTransformer to work around missing
   * EditableDatasetPropertiesTemplate on older DataHub servers (&lt;= v1.3.0).
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Fetch current editableDatasetProperties aspect from server
   *   <li>Apply patch operations to the fetched aspect locally
   *   <li>Return full aspect MCP with UPSERT change type
   * </ol>
   *
   * @param patch the editableDatasetProperties patch MCP to transform
   * @param client the EntityClient for fetching current aspect
   * @return full aspect replacement MCP
   * @throws IOException if aspect fetch or JSON processing fails
   * @throws ExecutionException if future execution fails
   * @throws InterruptedException if waiting is interrupted
   */
  @Nonnull
  public static MetadataChangeProposal transformPatchToFullAspect(
      @Nonnull MetadataChangeProposal patch, @Nonnull EntityClient client)
      throws IOException, ExecutionException, InterruptedException {

    log.debug(
        "Transforming editableDatasetProperties patch to full aspect for entity: {}",
        patch.getEntityUrn().toString());

    // Step 1: Fetch current editableDatasetProperties aspect
    datahub.client.v2.operations.AspectWithMetadata<EditableDatasetProperties> aspectWithMetadata =
        client.getAspect(patch.getEntityUrn(), EditableDatasetProperties.class);
    EditableDatasetProperties currentProps = aspectWithMetadata.getAspect();
    if (currentProps == null) {
      log.debug("No existing editableDatasetProperties found, creating new aspect");
      currentProps = new EditableDatasetProperties();
    } else {
      log.debug("Fetched existing editableDatasetProperties");
    }

    // Step 2: Apply patch operations to current aspect
    EditableDatasetProperties updatedProps = applyPatchOperations(currentProps, patch);
    log.debug("After applying patch, updated editableDatasetProperties");

    // Step 3: Create full MCP with UPSERT
    MetadataChangeProposal fullMcp = new MetadataChangeProposal();
    fullMcp.setEntityType(patch.getEntityType());
    fullMcp.setEntityUrn(patch.getEntityUrn());
    fullMcp.setChangeType(ChangeType.UPSERT);
    fullMcp.setAspectName("editableDatasetProperties");

    // Serialize the aspect to GenericAspect using JSON format
    GenericAspect genericAspect = new GenericAspect();
    String jsonString = com.datahub.util.RecordUtils.toJsonString(updatedProps);
    genericAspect.setValue(ByteString.copyString(jsonString, StandardCharsets.UTF_8));
    genericAspect.setContentType("application/json");
    fullMcp.setAspect(genericAspect);

    log.debug("Created full MCP for editableDatasetProperties aspect");
    return fullMcp;
  }

  /**
   * Applies JSON Patch operations from a patch MCP to an EditableDatasetProperties aspect.
   *
   * <p>Handles ADD and REMOVE operations on the /description and /name paths.
   *
   * @param current the current EditableDatasetProperties aspect
   * @param patch the patch MCP containing operations
   * @return the updated EditableDatasetProperties aspect
   * @throws IOException if JSON processing fails
   */
  @Nonnull
  private static EditableDatasetProperties applyPatchOperations(
      @Nonnull EditableDatasetProperties current, @Nonnull MetadataChangeProposal patch)
      throws IOException {

    // Parse the patch aspect as JSON to extract operations
    JsonNode patchNode =
        OBJECT_MAPPER.readTree(patch.getAspect().getValue().asString(StandardCharsets.UTF_8));

    // The patch can be either:
    // 1. A direct array of operations: [{op: "add", path: "/name", value: "..."}, ...]
    // 2. An object with a "patch" field: {"patch": [{op: "add", path: "/name", value: "..."}, ...]}
    JsonNode patchArray = null;
    if (patchNode.isArray()) {
      // Direct array format (from AbstractMultiFieldPatchBuilder.buildPatch())
      patchArray = patchNode;
    } else if (patchNode.has("patch")) {
      // Wrapped format
      patchArray = patchNode.get("patch");
    }

    if (patchArray == null || !patchArray.isArray()) {
      log.warn(
          "Patch does not contain expected patch array, returning current aspect unchanged. Patch structure: {}",
          patchNode);
      return current;
    }

    // Apply each operation
    for (JsonNode operation : patchArray) {
      String op = operation.get("op").asText();
      String path = operation.get("path").asText();

      if ("/description".equals(path)) {
        if ("add".equals(op)) {
          JsonNode value = operation.get("value");
          if (value != null && value.isTextual()) {
            current.setDescription(value.asText());
            log.debug("Set description to: {}", value.asText());
          }
        } else if ("remove".equals(op)) {
          current.removeDescription();
          log.debug("Removed description");
        }
      } else if ("/name".equals(path)) {
        if ("add".equals(op)) {
          JsonNode value = operation.get("value");
          if (value != null && value.isTextual()) {
            current.setName(value.asText());
            log.debug("Set name to: {}", value.asText());
          }
        } else if ("remove".equals(op)) {
          current.removeName();
          log.debug("Removed name");
        }
      }
    }

    return current;
  }
}
