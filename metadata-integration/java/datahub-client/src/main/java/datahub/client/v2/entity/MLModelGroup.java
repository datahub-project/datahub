package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.MlModelGroupUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.EditableMLModelGroupPropertiesPatchBuilder;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
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
 * Represents a DataHub ML Model Group entity with fluent builder API.
 *
 * <p>An ML Model Group represents a collection of versioned ML models (e.g., different versions of
 * the same model family). It provides a way to group related models and track their lineage through
 * training and downstream jobs.
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>The SDK supports two operation modes:
 *
 * <ul>
 *   <li>SDK mode (default): User-initiated edits write to editable aspects
 *   <li>INGESTION mode: System/pipeline writes write to system aspects
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * MLModelGroup modelGroup = MLModelGroup.builder()
 *     .platform("mlflow")
 *     .groupId("recommender-model")
 *     .env("PROD")
 *     .name("Recommender Model Family")
 *     .description("Customer product recommendation models")
 *     .build();
 *
 * // Add metadata (creates patches)
 * modelGroup.addTag("recommendation");
 * modelGroup.addOwner("urn:li:corpuser:ml-team", OwnershipType.TECHNICAL_OWNER);
 * modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_job_123");
 * modelGroup.setDomain("urn:li:domain:MachineLearning");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(modelGroup);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class MLModelGroup extends Entity
    implements HasTags<MLModelGroup>,
        HasGlossaryTerms<MLModelGroup>,
        HasOwners<MLModelGroup>,
        HasDomains<MLModelGroup>,
        HasSubTypes<MLModelGroup>,
        HasStructuredProperties<MLModelGroup> {

  private static final String ENTITY_TYPE = "mlModelGroup";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Constructs a new MLModelGroup entity.
   *
   * @param urn the ML model group URN
   */
  private MLModelGroup(@Nonnull MlModelGroupUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new MLModelGroup entity from generic URN.
   *
   * @param urn the ML model group URN (generic type)
   */
  protected MLModelGroup(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs an MLModelGroup loaded from server.
   *
   * @param urn the ML model group URN
   * @param aspects current aspects from server
   */
  private MLModelGroup(
      @Nonnull MlModelGroupUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs an MLModelGroup loaded from server with generic URN.
   *
   * @param urn the ML model group URN (generic type)
   * @param aspects current aspects from server
   */
  protected MLModelGroup(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  protected MLModelGroup(@Nonnull MLModelGroup other) {
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
        MLModelGroupProperties.class,
        com.linkedin.ml.metadata.EditableMLModelGroupProperties.class);
  }

  @Override
  @Nonnull
  public MLModelGroup mutable() {
    if (!readOnly) {
      return this;
    }
    return new MLModelGroup(this);
  }

  /**
   * Returns the ML model group URN.
   *
   * @return the ML model group URN
   */
  @Nonnull
  public MlModelGroupUrn getMlModelGroupUrn() {
    return (MlModelGroupUrn) urn;
  }

  /**
   * Creates a new builder for MLModelGroup.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<MLModelGroup> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<MLModelGroup> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<MLModelGroup> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<MLModelGroup> interface

  // ==================== Description Operations (Mode-Aware) ====================

  /**
   * Sets the description for this ML model group (mode-aware).
   *
   * <p>In SDK mode, writes to editableMLModelGroupProperties. In INGESTION mode, would write to
   * system aspects (not yet implemented for ML model groups).
   *
   * @param description the description
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    EditableMLModelGroupPropertiesPatchBuilder patch =
        new EditableMLModelGroupPropertiesPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added description patch");
    return this;
  }

  /**
   * Gets the description for this ML model group.
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null ? props.getDescription() : null;
  }

  // ==================== Name Operations ====================

  /**
   * Gets the display name for this ML model group.
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getName() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null ? props.getName() : null;
  }

  // ==================== External URL Operations ====================

  /**
   * Gets the external URL for this ML model group.
   *
   * @return the external URL, or null if not set
   */
  @Nullable
  public String getExternalUrl() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null && props.getExternalUrl() != null
        ? props.getExternalUrl().toString()
        : null;
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Gets custom properties for this ML model group.
   *
   * @return the custom properties map, or null if not set
   */
  @Nullable
  public Map<String, String> getCustomProperties() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null && props.hasCustomProperties() ? props.getCustomProperties() : null;
  }

  // ==================== Timestamp Operations ====================

  /**
   * Gets the created timestamp for this ML model group.
   *
   * @return the created timestamp, or null if not set
   */
  @Nullable
  public TimeStamp getCreated() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null ? props.getCreated() : null;
  }

  /**
   * Gets the last modified timestamp for this ML model group.
   *
   * @return the last modified timestamp, or null if not set
   */
  @Nullable
  public TimeStamp getLastModified() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    return props != null ? props.getLastModified() : null;
  }

  // ==================== Training Jobs Operations ====================

  /**
   * Gets the training jobs for this ML model group.
   *
   * @return list of training job URNs, or null if not set
   */
  @Nullable
  public List<String> getTrainingJobs() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    if (props != null && props.hasTrainingJobs()) {
      List<String> jobs = new ArrayList<>();
      for (Urn urn : props.getTrainingJobs()) {
        jobs.add(urn.toString());
      }
      return jobs;
    }
    return null;
  }

  /**
   * Sets the training jobs for this ML model group (replaces all).
   *
   * <p>Note: This directly caches the aspect since there's no patch builder for training jobs.
   *
   * @param trainingJobUrns list of training job URNs
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup setTrainingJobs(@Nonnull List<String> trainingJobUrns) {
    checkNotReadOnly("set training jobs");
    try {
      UrnArray jobs = new UrnArray();
      for (String jobUrn : trainingJobUrns) {
        jobs.add(Urn.createFromString(jobUrn));
      }

      MLModelGroupProperties props = getOrCreateMLModelGroupProperties();
      props.setTrainingJobs(jobs, SetMode.IGNORE_NULL);

      String aspectName = getAspectName(MLModelGroupProperties.class);
      cache.put(aspectName, props, AspectSource.LOCAL, true);
      log.debug("Set training jobs: {} jobs", trainingJobUrns.size());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid training job URN", e);
    }
    return this;
  }

  /**
   * Adds a training job to this ML model group.
   *
   * @param trainingJobUrn the training job URN
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup addTrainingJob(@Nonnull String trainingJobUrn) {
    checkNotReadOnly("add training job");
    try {
      Urn jobUrn = Urn.createFromString(trainingJobUrn);

      MLModelGroupProperties props = getOrCreateMLModelGroupProperties();
      UrnArray jobs = props.hasTrainingJobs() ? props.getTrainingJobs() : new UrnArray();
      if (!jobs.contains(jobUrn)) {
        jobs.add(jobUrn);
        props.setTrainingJobs(jobs, SetMode.IGNORE_NULL);

        String aspectName = getAspectName(MLModelGroupProperties.class);
        cache.put(aspectName, props, AspectSource.LOCAL, true);
        log.debug("Added training job: {}", trainingJobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid training job URN: " + trainingJobUrn, e);
    }
    return this;
  }

  /**
   * Removes a training job from this ML model group.
   *
   * @param trainingJobUrn the training job URN to remove
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup removeTrainingJob(@Nonnull String trainingJobUrn) {
    checkNotReadOnly("remove training job");
    try {
      Urn jobUrn = Urn.createFromString(trainingJobUrn);

      MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
      if (props != null && props.hasTrainingJobs()) {
        UrnArray jobs = new UrnArray();
        for (Urn urn : props.getTrainingJobs()) {
          if (!urn.equals(jobUrn)) {
            jobs.add(urn);
          }
        }
        props.setTrainingJobs(jobs, SetMode.IGNORE_NULL);

        String aspectName = getAspectName(MLModelGroupProperties.class);
        cache.put(aspectName, props, AspectSource.LOCAL, true);
        log.debug("Removed training job: {}", trainingJobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid training job URN: " + trainingJobUrn, e);
    }
    return this;
  }

  // ==================== Downstream Jobs Operations ====================

  /**
   * Gets the downstream jobs for this ML model group.
   *
   * @return list of downstream job URNs, or null if not set
   */
  @Nullable
  public List<String> getDownstreamJobs() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    if (props != null && props.hasDownstreamJobs()) {
      List<String> jobs = new ArrayList<>();
      for (Urn urn : props.getDownstreamJobs()) {
        jobs.add(urn.toString());
      }
      return jobs;
    }
    return null;
  }

  /**
   * Sets the downstream jobs for this ML model group (replaces all).
   *
   * @param downstreamJobUrns list of downstream job URNs
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup setDownstreamJobs(@Nonnull List<String> downstreamJobUrns) {
    checkNotReadOnly("set downstream jobs");
    try {
      UrnArray jobs = new UrnArray();
      for (String jobUrn : downstreamJobUrns) {
        jobs.add(Urn.createFromString(jobUrn));
      }

      MLModelGroupProperties props = getOrCreateMLModelGroupProperties();
      props.setDownstreamJobs(jobs, SetMode.IGNORE_NULL);

      String aspectName = getAspectName(MLModelGroupProperties.class);
      cache.put(aspectName, props, AspectSource.LOCAL, true);
      log.debug("Set downstream jobs: {} jobs", downstreamJobUrns.size());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid downstream job URN", e);
    }
    return this;
  }

  /**
   * Adds a downstream job to this ML model group.
   *
   * @param downstreamJobUrn the downstream job URN
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup addDownstreamJob(@Nonnull String downstreamJobUrn) {
    checkNotReadOnly("add downstream job");
    try {
      Urn jobUrn = Urn.createFromString(downstreamJobUrn);

      MLModelGroupProperties props = getOrCreateMLModelGroupProperties();
      UrnArray jobs = props.hasDownstreamJobs() ? props.getDownstreamJobs() : new UrnArray();
      if (!jobs.contains(jobUrn)) {
        jobs.add(jobUrn);
        props.setDownstreamJobs(jobs, SetMode.IGNORE_NULL);

        String aspectName = getAspectName(MLModelGroupProperties.class);
        cache.put(aspectName, props, AspectSource.LOCAL, true);
        log.debug("Added downstream job: {}", downstreamJobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid downstream job URN: " + downstreamJobUrn, e);
    }
    return this;
  }

  /**
   * Removes a downstream job from this ML model group.
   *
   * @param downstreamJobUrn the downstream job URN to remove
   * @return this ML model group
   */
  @RequiresMutable
  @Nonnull
  public MLModelGroup removeDownstreamJob(@Nonnull String downstreamJobUrn) {
    checkNotReadOnly("remove downstream job");
    try {
      Urn jobUrn = Urn.createFromString(downstreamJobUrn);

      MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
      if (props != null && props.hasDownstreamJobs()) {
        UrnArray jobs = new UrnArray();
        for (Urn urn : props.getDownstreamJobs()) {
          if (!urn.equals(jobUrn)) {
            jobs.add(urn);
          }
        }
        props.setDownstreamJobs(jobs, SetMode.IGNORE_NULL);

        String aspectName = getAspectName(MLModelGroupProperties.class);
        cache.put(aspectName, props, AspectSource.LOCAL, true);
        log.debug("Removed downstream job: {}", downstreamJobUrn);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid downstream job URN: " + downstreamJobUrn, e);
    }
    return this;
  }

  // ==================== Helper Methods ====================

  /**
   * Gets or creates MLModelGroupProperties aspect.
   *
   * @return the MLModelGroupProperties aspect
   */
  @Nonnull
  private MLModelGroupProperties getOrCreateMLModelGroupProperties() {
    MLModelGroupProperties props = getAspectLazy(MLModelGroupProperties.class);
    if (props == null) {
      props = new MLModelGroupProperties();
      String aspectName = getAspectName(MLModelGroupProperties.class);
      cache.put(aspectName, props, AspectSource.LOCAL, true);
    }
    return props;
  }

  // ==================== Builder ====================

  /** Builder for creating MLModelGroup entities with a fluent API. */
  public static class Builder {
    private String platform;
    private String groupId;
    private String env = "PROD";
    private String name;
    private String description;
    private String externalUrl;
    private Map<String, String> customProperties;
    private TimeStamp created;
    private TimeStamp lastModified;
    private List<String> trainingJobs;
    private List<String> downstreamJobs;

    /**
     * Sets the platform name (required).
     *
     * @param platform the platform name (e.g., "mlflow", "sagemaker", "vertexai")
     * @return this builder
     */
    @Nonnull
    public Builder platform(@Nonnull String platform) {
      this.platform = platform;
      return this;
    }

    /**
     * Sets the model group ID (required).
     *
     * @param groupId the model group ID (e.g., "recommender-model", "fraud-detector")
     * @return this builder
     */
    @Nonnull
    public Builder groupId(@Nonnull String groupId) {
      this.groupId = groupId;
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
     * Sets the display name.
     *
     * @param name the display name
     * @return this builder
     */
    @Nonnull
    public Builder name(@Nullable String name) {
      this.name = name;
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
     * Sets the created timestamp.
     *
     * @param created the created timestamp
     * @return this builder
     */
    @Nonnull
    public Builder created(@Nullable TimeStamp created) {
      this.created = created;
      return this;
    }

    /**
     * Sets the last modified timestamp.
     *
     * @param lastModified the last modified timestamp
     * @return this builder
     */
    @Nonnull
    public Builder lastModified(@Nullable TimeStamp lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    /**
     * Sets the training jobs.
     *
     * @param trainingJobs list of training job URNs
     * @return this builder
     */
    @Nonnull
    public Builder trainingJobs(@Nullable List<String> trainingJobs) {
      this.trainingJobs = trainingJobs;
      return this;
    }

    /**
     * Sets the downstream jobs.
     *
     * @param downstreamJobs list of downstream job URNs
     * @return this builder
     */
    @Nonnull
    public Builder downstreamJobs(@Nullable List<String> downstreamJobs) {
      this.downstreamJobs = downstreamJobs;
      return this;
    }

    /**
     * Sets the URN for this ML model group by extracting its components.
     *
     * @param urn the ML model group URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull MlModelGroupUrn urn) {
      this.platform = urn.getPlatformEntity().getPlatformNameEntity();
      this.groupId = urn.getNameEntity();
      this.env = urn.getOriginEntity().name();
      return this;
    }

    /**
     * Builds the MLModelGroup entity.
     *
     * <p>Note: Properties set in the builder are cached as aspects, not converted to patches. Use
     * the setter methods on the MLModelGroup after building if you want patch-based updates.
     *
     * @return the MLModelGroup entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public MLModelGroup build() {
      if (platform == null || groupId == null) {
        throw new IllegalArgumentException("platform and groupId are required");
      }

      try {
        DataPlatformUrn platformUrn = new DataPlatformUrn(platform);
        FabricType fabricType = FabricType.valueOf(env);

        MlModelGroupUrn urn = new MlModelGroupUrn(platformUrn, groupId, fabricType);
        MLModelGroup modelGroup = new MLModelGroup(urn);

        // Cache MLModelGroupProperties aspect if any properties are provided
        if (name != null
            || description != null
            || externalUrl != null
            || customProperties != null
            || created != null
            || lastModified != null
            || trainingJobs != null
            || downstreamJobs != null) {

          MLModelGroupProperties props = new MLModelGroupProperties();

          if (name != null) {
            props.setName(name);
          }
          if (description != null) {
            props.setDescription(description);
          }
          if (externalUrl != null) {
            try {
              props.setExternalUrl(new com.linkedin.common.url.Url(externalUrl));
            } catch (Exception e) {
              throw new IllegalArgumentException("Invalid external URL: " + externalUrl, e);
            }
          }
          if (customProperties != null) {
            props.setCustomProperties(new StringMap(customProperties));
          }
          if (created != null) {
            props.setCreated(created);
          }
          if (lastModified != null) {
            props.setLastModified(lastModified);
          }
          if (trainingJobs != null && !trainingJobs.isEmpty()) {
            UrnArray jobs = new UrnArray();
            for (String jobUrn : trainingJobs) {
              jobs.add(Urn.createFromString(jobUrn));
            }
            props.setTrainingJobs(jobs);
          }
          if (downstreamJobs != null && !downstreamJobs.isEmpty()) {
            UrnArray jobs = new UrnArray();
            for (String jobUrn : downstreamJobs) {
              jobs.add(Urn.createFromString(jobUrn));
            }
            props.setDownstreamJobs(jobs);
          }

          String aspectName = modelGroup.getAspectName(MLModelGroupProperties.class);
          modelGroup.cache.put(aspectName, props, AspectSource.LOCAL, true);
        }

        return modelGroup;
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create ML model group URN", e);
      }
    }
  }

  // ==================== Patch Transformation (Version Compatibility) ====================

  /**
   * Transforms an editableMLModelGroupProperties patch MCP to a full aspect replacement MCP via
   * read-modify-write.
   *
   * <p>This is used by the VersionAwarePatchTransformer to work around missing
   * EditableMLModelGroupPropertiesTemplate on older DataHub servers (&lt;= v1.3.0).
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Fetch current editableMLModelGroupProperties aspect from server
   *   <li>Apply patch operations to the fetched aspect locally
   *   <li>Return full aspect MCP with UPSERT change type
   * </ol>
   *
   * @param patch the editableMLModelGroupProperties patch MCP to transform
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
        "Transforming editableMLModelGroupProperties patch to full aspect for entity: {}",
        patch.getEntityUrn().toString());

    // Step 1: Fetch current editableMLModelGroupProperties aspect
    datahub.client.v2.operations.AspectWithMetadata<EditableMLModelGroupProperties>
        aspectWithMetadata =
            client.getAspect(patch.getEntityUrn(), EditableMLModelGroupProperties.class);
    EditableMLModelGroupProperties currentProps = aspectWithMetadata.getAspect();
    if (currentProps == null) {
      log.debug("No existing editableMLModelGroupProperties found, creating new aspect");
      currentProps = new EditableMLModelGroupProperties();
    } else {
      log.debug("Fetched existing editableMLModelGroupProperties");
    }

    // Step 2: Apply patch operations to current aspect
    EditableMLModelGroupProperties updatedProps = applyPatchOperations(currentProps, patch);
    log.debug("After applying patch, updated editableMLModelGroupProperties");

    // Step 3: Create full MCP with UPSERT
    MetadataChangeProposal fullMcp = new MetadataChangeProposal();
    fullMcp.setEntityType(patch.getEntityType());
    fullMcp.setEntityUrn(patch.getEntityUrn());
    fullMcp.setChangeType(ChangeType.UPSERT);
    fullMcp.setAspectName("editableMLModelGroupProperties");

    // Serialize the aspect to GenericAspect
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.copyString(
            RecordTemplate.class.cast(updatedProps).data().toString(), StandardCharsets.UTF_8));
    genericAspect.setContentType("application/json");
    fullMcp.setAspect(genericAspect);

    log.debug("Created full MCP for editableMLModelGroupProperties aspect");
    return fullMcp;
  }

  /**
   * Applies JSON Patch operations from a patch MCP to an EditableMLModelGroupProperties aspect.
   *
   * <p>Handles ADD and REMOVE operations on the /description path.
   *
   * @param current the current EditableMLModelGroupProperties aspect
   * @param patch the patch MCP containing operations
   * @return the updated EditableMLModelGroupProperties aspect
   * @throws IOException if JSON processing fails
   */
  @Nonnull
  private static EditableMLModelGroupProperties applyPatchOperations(
      @Nonnull EditableMLModelGroupProperties current, @Nonnull MetadataChangeProposal patch)
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
            log.debug("Set description to: {}", value.asText());
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
