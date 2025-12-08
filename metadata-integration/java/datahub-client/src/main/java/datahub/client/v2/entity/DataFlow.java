package datahub.client.v2.entity;

import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.patch.builder.DataFlowInfoPatchBuilder;
import datahub.client.v2.annotations.RequiresMutable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub DataFlow entity with fluent builder API.
 *
 * <p>A DataFlow represents a data processing pipeline or workflow (e.g., an Airflow DAG, Spark job,
 * dbt project).
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * DataFlow dataflow = DataFlow.builder()
 *     .orchestrator("airflow")
 *     .flowId("my_dag_id")
 *     .cluster("prod")
 *     .displayName("My ETL Pipeline")
 *     .description("Daily ETL pipeline for customer data")
 *     .build();
 *
 * // Add metadata (creates patches)
 * dataflow.addTag("pii");
 * dataflow.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
 * dataflow.addTerm("urn:li:glossaryTerm:ETL");
 * dataflow.setDomain("urn:li:domain:DataEngineering");
 *
 * // Upsert to DataHub (emits accumulated patches)
 * client.entities().upsert(dataflow);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class DataFlow extends Entity
    implements HasTags<DataFlow>,
        HasGlossaryTerms<DataFlow>,
        HasOwners<DataFlow>,
        HasDomains<DataFlow>,
        HasSubTypes<DataFlow>,
        HasStructuredProperties<DataFlow> {

  private static final String ENTITY_TYPE = "dataFlow";

  /**
   * Constructs a new DataFlow entity.
   *
   * @param urn the dataflow URN
   */
  private DataFlow(@Nonnull DataFlowUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new DataFlow entity from generic URN.
   *
   * @param urn the dataflow URN (generic type)
   */
  protected DataFlow(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs a DataFlow loaded from server.
   *
   * @param urn the dataflow URN
   * @param aspects current aspects from server
   */
  private DataFlow(
      @Nonnull DataFlowUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs a DataFlow loaded from server with generic URN.
   *
   * @param urn the dataflow URN (generic type)
   * @param aspects current aspects from server
   */
  protected DataFlow(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  protected DataFlow(@Nonnull DataFlow other) {
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
  public DataFlow mutable() {
    if (!readOnly) {
      return this;
    }
    return new DataFlow(this);
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
        DataFlowInfo.class,
        com.linkedin.datajob.EditableDataFlowProperties.class);
  }

  /**
   * Returns the dataflow URN.
   *
   * @return the dataflow URN
   */
  @Nonnull
  public DataFlowUrn getDataFlowUrn() {
    return (DataFlowUrn) urn;
  }

  /**
   * Creates a new builder for DataFlow.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Gets or creates the accumulated DataFlowInfoPatchBuilder for this entity.
   *
   * <p>This builder accumulates multiple patch operations into a single MCP, improving efficiency.
   *
   * @return the accumulated patch builder
   */
  @Nonnull
  private DataFlowInfoPatchBuilder getDataFlowInfoPatchBuilder() {
    DataFlowInfoPatchBuilder builder =
        getPatchBuilder("dataFlowInfo", DataFlowInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DataFlowInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dataFlowInfo", builder);
    }
    return builder;
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<DataFlow> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<DataFlow> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<DataFlow> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<DataFlow> interface

  // ==================== Description Operations ====================

  /**
   * Sets the description for this dataflow using patch-based update.
   *
   * @param description the description
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    getDataFlowInfoPatchBuilder().setDescription(description);
    log.debug("Added description to accumulated patch");
    return this;
  }

  /**
   * Gets the description for this dataflow.
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null ? info.getDescription() : null;
  }

  // ==================== Display Name Operations ====================

  /**
   * Sets the display name for this dataflow using patch-based update.
   *
   * @param displayName the display name
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setDisplayName(@Nonnull String displayName) {
    checkNotReadOnly("set display name");
    getDataFlowInfoPatchBuilder().setName(displayName);
    log.debug("Added display name to accumulated patch");
    return this;
  }

  /**
   * Gets the display name for this dataflow.
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getDisplayName() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null ? info.getName() : null;
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Adds a custom property to this dataflow.
   *
   * @param key the property key
   * @param value the property value
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    getDataFlowInfoPatchBuilder().addCustomProperty(key, value);
    log.debug("Added custom property to accumulated patch: {}={}", key, value);
    return this;
  }

  /**
   * Removes a custom property from this dataflow.
   *
   * @param key the property key to remove
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow removeCustomProperty(@Nonnull String key) {
    checkNotReadOnly("remove custom property");
    getDataFlowInfoPatchBuilder().removeCustomProperty(key);
    log.debug("Added remove custom property to accumulated patch: {}", key);
    return this;
  }

  /**
   * Sets custom properties for this dataflow (replaces all).
   *
   * @param properties map of custom properties
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    getDataFlowInfoPatchBuilder().setCustomProperties(properties);
    log.debug(
        "Added set custom properties to accumulated patch with {} properties", properties.size());
    return this;
  }

  // ==================== DataFlow-Specific Properties ====================

  /**
   * Sets the external URL for this dataflow.
   *
   * <p>This URL can be used as an external link on DataHub to allow users to access/view the
   * dataflow in the source orchestration tool.
   *
   * @param externalUrl the external URL
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setExternalUrl(@Nonnull String externalUrl) {
    checkNotReadOnly("set external URL");
    DataFlowInfo flowInfo = getOrCreateDataFlowInfo();
    flowInfo.setExternalUrl(new com.linkedin.common.url.Url(externalUrl));
    String aspectName = getAspectName(DataFlowInfo.class);
    cache.put(aspectName, flowInfo, AspectSource.LOCAL, true);
    log.debug("Set external URL");
    return this;
  }

  /**
   * Gets the external URL for this dataflow.
   *
   * @return the external URL, or null if not set
   */
  @Nullable
  public String getExternalUrl() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null && info.getExternalUrl() != null ? info.getExternalUrl().toString() : null;
  }

  /**
   * Sets the project for this dataflow.
   *
   * @param project the project name
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setProject(@Nonnull String project) {
    checkNotReadOnly("set project");
    DataFlowInfo flowInfo = getOrCreateDataFlowInfo();
    flowInfo.setProject(project);
    String aspectName = getAspectName(DataFlowInfo.class);
    cache.put(aspectName, flowInfo, AspectSource.LOCAL, true);
    log.debug("Set project via direct mutation");
    return this;
  }

  /**
   * Gets the project for this dataflow.
   *
   * @return the project, or null if not set
   */
  @Nullable
  public String getProject() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null ? info.getProject() : null;
  }

  /**
   * Sets the created timestamp for this dataflow.
   *
   * @param createdTimeMs the timestamp in milliseconds since epoch
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setCreated(long createdTimeMs) {
    checkNotReadOnly("set created");
    TimeStamp created = new TimeStamp();
    created.setTime(createdTimeMs);
    getDataFlowInfoPatchBuilder().setCreated(created);
    log.debug("Added created timestamp to accumulated patch");
    return this;
  }

  /**
   * Gets the created timestamp for this dataflow.
   *
   * @return the timestamp in milliseconds since epoch, or null if not set
   */
  @Nullable
  public Long getCreated() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null && info.getCreated() != null ? info.getCreated().getTime() : null;
  }

  /**
   * Sets the last modified timestamp for this dataflow.
   *
   * @param lastModifiedTimeMs the timestamp in milliseconds since epoch
   * @return this dataflow
   */
  @RequiresMutable
  @Nonnull
  public DataFlow setLastModified(long lastModifiedTimeMs) {
    checkNotReadOnly("set last modified");
    TimeStamp lastModified = new TimeStamp();
    lastModified.setTime(lastModifiedTimeMs);
    getDataFlowInfoPatchBuilder().setLastModified(lastModified);
    log.debug("Added last modified timestamp to accumulated patch");
    return this;
  }

  /**
   * Gets the last modified timestamp for this dataflow.
   *
   * @return the timestamp in milliseconds since epoch, or null if not set
   */
  @Nullable
  public Long getLastModified() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    return info != null && info.getLastModified() != null ? info.getLastModified().getTime() : null;
  }

  /**
   * Helper method to get or create DataFlowInfo aspect.
   *
   * @return the DataFlowInfo aspect
   */
  @Nonnull
  private DataFlowInfo getOrCreateDataFlowInfo() {
    DataFlowInfo info = getAspectLazy(DataFlowInfo.class);
    if (info == null) {
      info = new DataFlowInfo();
      info.setName(getDataFlowUrn().getFlowIdEntity());
    }
    return info;
  }

  // ==================== Builder ====================

  public static class Builder {
    private String orchestrator;
    private String flowId;
    private String cluster;
    private String displayName;
    private String description;
    private Map<String, String> customProperties;

    @Nonnull
    public Builder orchestrator(@Nonnull String orchestrator) {
      this.orchestrator = orchestrator;
      return this;
    }

    @Nonnull
    public Builder flowId(@Nonnull String flowId) {
      this.flowId = flowId;
      return this;
    }

    @Nonnull
    public Builder cluster(@Nonnull String cluster) {
      this.cluster = cluster;
      return this;
    }

    @Nonnull
    public Builder displayName(@Nullable String displayName) {
      this.displayName = displayName;
      return this;
    }

    @Nonnull
    public Builder description(@Nullable String description) {
      this.description = description;
      return this;
    }

    @Nonnull
    public Builder customProperties(@Nullable Map<String, String> properties) {
      this.customProperties = properties;
      return this;
    }

    /**
     * Sets the URN for this dataflow by extracting its components.
     *
     * @param urn the dataflow URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull DataFlowUrn urn) {
      this.orchestrator = urn.getOrchestratorEntity();
      this.flowId = urn.getFlowIdEntity();
      this.cluster = urn.getClusterEntity();
      return this;
    }

    @Nonnull
    public DataFlow build() {
      if (orchestrator == null || flowId == null || cluster == null) {
        throw new IllegalArgumentException("orchestrator, flowId, and cluster are required");
      }

      DataFlowUrn urn = new DataFlowUrn(orchestrator, flowId, cluster);
      DataFlow dataflow = new DataFlow(urn);

      if (displayName != null || description != null || customProperties != null) {
        DataFlowInfo info = new DataFlowInfo();
        if (displayName != null) {
          info.setName(displayName);
        } else {
          info.setName(flowId);
        }
        if (description != null) {
          info.setDescription(description);
        }
        if (customProperties != null) {
          info.setCustomProperties(new StringMap(customProperties));
        }
        String aspectName = dataflow.getAspectName(DataFlowInfo.class);
        dataflow.cache.put(aspectName, info, AspectSource.LOCAL, true);
      }

      return dataflow;
    }
  }
}
