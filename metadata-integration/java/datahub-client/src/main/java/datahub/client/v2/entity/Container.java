package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.EditableContainerPropertiesPatchBuilder;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.annotations.RequiresMutable;
import datahub.client.v2.operations.EntityClient;
import io.datahubproject.models.util.DataHubGuidGenerator;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub Container entity with fluent builder API.
 *
 * <p>A Container represents a hierarchical grouping of data assets, such as:
 *
 * <ul>
 *   <li>Databases containing schemas and tables
 *   <li>Schemas containing tables
 *   <li>Folders or buckets containing files
 *   <li>Projects containing datasets
 * </ul>
 *
 * <p>Containers use a GUID-based URN structure: {@code urn:li:container:{guid}}. The GUID is
 * generated from a set of properties (platform, database, schema, etc.) to ensure deterministic
 * URNs for the same logical container.
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * // Create a database container
 * Container database = Container.builder()
 *     .platform("snowflake")
 *     .database("my_database")
 *     .env("PROD")
 *     .displayName("My Database")
 *     .description("Production database")
 *     .build();
 *
 * // Create a schema container with parent
 * Container schema = Container.builder()
 *     .platform("snowflake")
 *     .database("my_database")
 *     .schema("public")
 *     .env("PROD")
 *     .displayName("Public Schema")
 *     .parentContainer(database.getUrn().toString())
 *     .build();
 *
 * // Add metadata (creates patches)
 * database.addTag("production");
 * database.addOwner("urn:li:corpuser:johndoe", OwnershipType.DATA_STEWARD);
 * database.setDomain("urn:li:domain:Engineering");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(database);
 * client.entities().upsert(schema);
 * </pre>
 *
 * @see Entity
 * @see HasContainer
 */
@Slf4j
public class Container extends Entity
    implements HasContainer<Container>,
        HasTags<Container>,
        HasGlossaryTerms<Container>,
        HasOwners<Container>,
        HasDomains<Container>,
        HasSubTypes<Container>,
        HasStructuredProperties<Container> {

  private static final String ENTITY_TYPE = "container";

  /**
   * Constructs a new Container entity.
   *
   * @param urn the container URN
   */
  private Container(@Nonnull Urn urn) {
    super(urn);
  }

  /**
   * Constructs a Container loaded from server.
   *
   * @param urn the container URN
   * @param aspects current aspects from server
   */
  private Container(
      @Nonnull Urn urn, @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  protected Container(@Nonnull Container other) {
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
  public Container mutable() {
    if (!readOnly) {
      return this;
    }
    return new Container(this);
  }

  @Override
  @Nonnull
  public java.util.List<Class<? extends com.linkedin.data.template.RecordTemplate>>
      getDefaultAspects() {
    return java.util.List.of(
        com.linkedin.common.Ownership.class,
        com.linkedin.common.GlobalTags.class,
        com.linkedin.common.GlossaryTerms.class,
        com.linkedin.domain.Domains.class,
        com.linkedin.common.Status.class,
        com.linkedin.common.InstitutionalMemory.class,
        ContainerProperties.class,
        com.linkedin.container.EditableContainerProperties.class);
  }

  /**
   * Returns the container URN as a string.
   *
   * @return the container URN string
   */
  @Nonnull
  public String getContainerUrn() {
    return urn.toString();
  }

  /**
   * Creates a new builder for Container.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<Container> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<Container> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<Container> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<Container> interface

  // ==================== Description Operations ====================

  /**
   * Sets the description for this container using patch-based update.
   *
   * <p>Updates the editable description (editableContainerProperties aspect).
   *
   * @param description the description
   * @return this container
   */
  @RequiresMutable
  @Nonnull
  public Container setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    EditableContainerPropertiesPatchBuilder patch =
        new EditableContainerPropertiesPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added description patch");
    return this;
  }

  /**
   * Gets the description for this container (lazy-loaded).
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    ContainerProperties props = getAspectLazy(ContainerProperties.class);
    return props != null ? props.getDescription() : null;
  }

  // ==================== Display Name Operations ====================

  /**
   * Gets the display name for this container (lazy-loaded).
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getDisplayName() {
    ContainerProperties props = getAspectLazy(ContainerProperties.class);
    return props != null ? props.getName() : null;
  }

  // ==================== Qualified Name Operations ====================

  /**
   * Gets the qualified name for this container (lazy-loaded).
   *
   * @return the qualified name, or null if not set
   */
  @Nullable
  public String getQualifiedName() {
    ContainerProperties props = getAspectLazy(ContainerProperties.class);
    return props != null ? props.getQualifiedName() : null;
  }

  // ==================== External URL Operations ====================

  /**
   * Gets the external URL for this container (lazy-loaded).
   *
   * @return the external URL, or null if not set
   */
  @Nullable
  public String getExternalUrl() {
    ContainerProperties props = getAspectLazy(ContainerProperties.class);
    return props != null && props.getExternalUrl() != null
        ? props.getExternalUrl().toString()
        : null;
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Gets the custom properties for this container (lazy-loaded).
   *
   * @return map of custom properties, or null if not set
   */
  @Nullable
  public Map<String, String> getCustomProperties() {
    ContainerProperties props = getAspectLazy(ContainerProperties.class);
    return props != null && props.getCustomProperties() != null
        ? new HashMap<>(props.getCustomProperties())
        : null;
  }

  // ==================== Container Hierarchy Operations ====================
  // Provided by HasContainer<Container> interface

  /**
   * Gets the parent container URN for this container.
   *
   * @return the parent container URN, or null if not set
   */
  @Nullable
  public String getParentContainer() {
    com.linkedin.container.Container containerAspect =
        getAspectLazy(com.linkedin.container.Container.class);
    return containerAspect != null && containerAspect.getContainer() != null
        ? containerAspect.getContainer().toString()
        : null;
  }

  // ==================== Builder ====================

  /**
   * Builder for creating Container entities with a fluent API.
   *
   * <p>Containers are identified by a GUID generated from their properties (platform, database,
   * schema, etc.). This ensures that the same logical container always gets the same URN.
   */
  public static class Builder {
    private Urn urn;
    private String platform;
    private String database;
    private String schema;
    private String env = "PROD";
    private String platformInstance;
    private String displayName;
    private String qualifiedName;
    private String description;
    private String externalUrl;
    private String parentContainer;
    private Map<String, String> customProperties;

    /**
     * Sets the platform name (required).
     *
     * @param platform the platform name (e.g., "snowflake", "bigquery", "postgres")
     * @return this builder
     */
    @Nonnull
    public Builder platform(@Nonnull String platform) {
      this.platform = platform;
      return this;
    }

    /**
     * Sets the database name.
     *
     * @param database the database name
     * @return this builder
     */
    @Nonnull
    public Builder database(@Nullable String database) {
      this.database = database;
      return this;
    }

    /**
     * Sets the schema name.
     *
     * <p>When setting a schema, you should typically also set the database name.
     *
     * @param schema the schema name
     * @return this builder
     */
    @Nonnull
    public Builder schema(@Nullable String schema) {
      this.schema = schema;
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
     * Sets the display name (required).
     *
     * @param displayName the display name
     * @return this builder
     */
    @Nonnull
    public Builder displayName(@Nonnull String displayName) {
      this.displayName = displayName;
      return this;
    }

    /**
     * Sets the qualified name.
     *
     * <p>A fully-qualified name for the container (e.g., "prod.my_database.public").
     *
     * @param qualifiedName the qualified name
     * @return this builder
     */
    @Nonnull
    public Builder qualifiedName(@Nullable String qualifiedName) {
      this.qualifiedName = qualifiedName;
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
     * Sets the external URL.
     *
     * @param externalUrl the external URL
     * @return this builder
     */
    @Nonnull
    public Builder externalUrl(@Nullable String externalUrl) {
      this.externalUrl = externalUrl;
      return this;
    }

    /**
     * Sets the parent container URN.
     *
     * <p>This establishes a hierarchical relationship (e.g., schema's parent is database).
     *
     * @param parentContainer the parent container URN
     * @return this builder
     */
    @Nonnull
    public Builder parentContainer(@Nullable String parentContainer) {
      this.parentContainer = parentContainer;
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
     * Sets the URN for this container directly, skipping GUID generation.
     *
     * <p>Use this when you have an existing container URN (e.g., from server fetch). When
     * specified, the builder will use this URN instead of generating a new GUID.
     *
     * @param urn the container URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull Urn urn) {
      this.urn = urn;
      return this;
    }

    /**
     * Builds the Container entity.
     *
     * <p>The container's URN is generated from a GUID based on its properties. At minimum, the
     * platform must be specified. For hierarchical containers (database, schema), include the
     * appropriate properties.
     *
     * @return the Container entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public Container build() {
      if (displayName == null) {
        throw new IllegalArgumentException("displayName is required");
      }

      try {
        Urn finalUrn;
        Map<String, String> guidProperties = new HashMap<>();

        // Use provided URN if available, otherwise generate from properties
        if (urn != null) {
          finalUrn = urn;
        } else {
          if (platform == null) {
            throw new IllegalArgumentException("platform is required when URN is not provided");
          }

          // Build the property map for GUID generation
          guidProperties.put("platform", platform);

          if (platformInstance != null) {
            guidProperties.put("instance", platformInstance);
          }

          if (database != null) {
            guidProperties.put("database", database);
          }

          if (schema != null) {
            guidProperties.put("schema", schema);
          }

          if (env != null) {
            guidProperties.put("env", env);
          }

          // Generate GUID and create URN
          String guid = DataHubGuidGenerator.dataHubGuid(guidProperties);
          String urnString = "urn:li:container:" + guid;
          finalUrn = Urn.createFromString(urnString);
        }

        Urn urn = finalUrn;
        Container container = new Container(urn);

        // Build ContainerProperties aspect
        ContainerProperties props = new ContainerProperties();
        props.setName(displayName);

        if (qualifiedName != null) {
          props.setQualifiedName(qualifiedName);
        }

        if (description != null) {
          props.setDescription(description);
        }

        if (externalUrl != null) {
          props.setExternalUrl(new com.linkedin.common.url.Url(externalUrl));
        }

        // Merge GUID properties with custom properties
        Map<String, String> allProperties = new HashMap<>(guidProperties);
        if (customProperties != null) {
          allProperties.putAll(customProperties);
        }
        props.setCustomProperties(new StringMap(allProperties));

        // Cache the aspect
        String aspectName = container.getAspectName(ContainerProperties.class);
        container.cache.put(aspectName, props, AspectSource.LOCAL, true);

        // Set parent container if provided
        if (parentContainer != null) {
          container.setContainer(parentContainer);
        }

        return container;
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create container URN", e);
      }
    }
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Transforms an editableContainerProperties patch MCP to a full aspect replacement MCP via
   * read-modify-write.
   *
   * <p>This is used by the VersionAwarePatchTransformer to work around missing
   * EditableContainerPropertiesTemplate on older DataHub servers (&lt;= v1.3.0).
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Fetch current editableContainerProperties aspect from server
   *   <li>Apply patch operations to the fetched aspect locally
   *   <li>Return full aspect MCP with UPSERT change type
   * </ol>
   *
   * @param patch the editableContainerProperties patch MCP to transform
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
        "Transforming editableContainerProperties patch to full aspect for entity: {}",
        patch.getEntityUrn().toString());

    // Step 1: Fetch current editableContainerProperties aspect
    datahub.client.v2.operations.AspectWithMetadata<EditableContainerProperties>
        aspectWithMetadata =
            client.getAspect(patch.getEntityUrn(), EditableContainerProperties.class);
    EditableContainerProperties currentProps = aspectWithMetadata.getAspect();
    if (currentProps == null) {
      log.debug("No existing editableContainerProperties found, creating new aspect");
      currentProps = new EditableContainerProperties();
    } else {
      log.debug("Fetched existing editableContainerProperties");
    }

    // Step 2: Apply patch operations to current aspect
    EditableContainerProperties updatedProps = applyPatchOperations(currentProps, patch);
    log.debug("Applied patch operations to editableContainerProperties");

    // Step 3: Create full MCP with UPSERT
    MetadataChangeProposal fullMcp = new MetadataChangeProposal();
    fullMcp.setEntityType(patch.getEntityType());
    fullMcp.setEntityUrn(patch.getEntityUrn());
    fullMcp.setChangeType(ChangeType.UPSERT);
    fullMcp.setAspectName("editableContainerProperties");

    // Serialize the aspect to GenericAspect
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.copyString(
            RecordTemplate.class.cast(updatedProps).data().toString(), StandardCharsets.UTF_8));
    genericAspect.setContentType("application/json");
    fullMcp.setAspect(genericAspect);

    log.debug("Created full MCP for editableContainerProperties aspect");
    return fullMcp;
  }

  /**
   * Applies JSON Patch operations from a patch MCP to an EditableContainerProperties aspect.
   *
   * <p>Handles ADD and REMOVE operations on the /description path.
   *
   * @param current the current EditableContainerProperties aspect
   * @param patch the patch MCP containing operations
   * @return the updated EditableContainerProperties aspect
   * @throws IOException if JSON processing fails
   */
  @Nonnull
  private static EditableContainerProperties applyPatchOperations(
      @Nonnull EditableContainerProperties current, @Nonnull MetadataChangeProposal patch)
      throws IOException {

    // Parse the patch aspect as JSON to extract operations
    JsonNode patchNode =
        OBJECT_MAPPER.readTree(patch.getAspect().getValue().asString(StandardCharsets.UTF_8));
    JsonNode patchArray = patchNode.get("patch");

    if (patchArray == null || !patchArray.isArray()) {
      log.warn("Patch does not contain expected 'patch' array, returning current aspect unchanged");
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
            log.debug("Set description");
          }
        } else if ("remove".equals(op)) {
          current.removeDescription();
          log.debug("Removed description");
        }
      }
    }

    return current;
  }
}
