package datahub.client.v2.entity;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.builder.ChartInfoPatchBuilder;
import datahub.client.v2.annotations.RequiresMutable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub Chart entity with fluent builder API.
 *
 * <p>A Chart represents a visualization or report in a BI tool (e.g., a chart in Looker, Tableau,
 * or Superset).
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * Chart chart = Chart.builder()
 *     .tool("looker")
 *     .id("my_chart_id")
 *     .title("Sales Performance Chart")
 *     .description("Monthly sales by region")
 *     .build();
 *
 * // Add metadata (creates patches)
 * chart.addTag("pii");
 * chart.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
 * chart.addTerm("urn:li:glossaryTerm:CustomerData");
 * chart.setDomain("urn:li:domain:Marketing");
 *
 * // Upsert to DataHub (emits accumulated patches)
 * client.entities().upsert(chart);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class Chart extends Entity
    implements HasTags<Chart>,
        HasGlossaryTerms<Chart>,
        HasOwners<Chart>,
        HasDomains<Chart>,
        HasSubTypes<Chart>,
        HasStructuredProperties<Chart> {

  private static final String ENTITY_TYPE = "chart";

  /**
   * Constructs a new Chart entity.
   *
   * @param urn the chart URN
   */
  private Chart(@Nonnull ChartUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new Chart entity from generic URN.
   *
   * @param urn the chart URN (generic type)
   */
  protected Chart(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs a Chart loaded from server.
   *
   * @param urn the chart URN
   * @param aspects current aspects from server
   */
  private Chart(
      @Nonnull ChartUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs a Chart loaded from server with generic URN.
   *
   * @param urn the chart URN (generic type)
   * @param aspects current aspects from server
   */
  protected Chart(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Copy constructor for creating mutable copy via .mutable().
   *
   * <p>This constructor creates a new Chart instance that shares immutable fields (urn, cache,
   * client, mode) with the original, but has fresh mutation tracking fields (pendingPatches,
   * pendingMCPs, patchBuilders) and is marked as mutable.
   *
   * @param other the Chart to copy from
   */
  protected Chart(@Nonnull Chart other) {
    super(
        other.urn, // Share URN
        other.cache, // Share cache (read from same server data)
        other.client, // Share client reference
        other.mode, // Share operation mode
        new HashMap<>(), // Fresh empty pending patches
        new ArrayList<>(), // Fresh empty pending MCPs
        new HashMap<>(), // Fresh empty patch builders
        false, // Not dirty
        false); // Mutable (readOnly = false)
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
        ChartInfo.class,
        com.linkedin.chart.EditableChartProperties.class);
  }

  /**
   * Returns a mutable copy of this Chart entity.
   *
   * <p>If this entity is already mutable, returns this instance (idempotent). If this entity is
   * read-only (fetched from server), creates and returns a new mutable copy that shares the aspect
   * cache and other immutable fields, but has independent mutation tracking.
   *
   * @return this instance if already mutable, or a new mutable copy if read-only
   */
  @Override
  @Nonnull
  public Chart mutable() {
    if (!readOnly) {
      return this; // Already mutable, return self (idempotent)
    }
    return new Chart(this); // Create mutable copy using copy constructor
  }

  /**
   * Returns the chart URN.
   *
   * @return the chart URN
   */
  @Nonnull
  public ChartUrn getChartUrn() {
    return (ChartUrn) urn;
  }

  /**
   * Creates a new builder for Chart.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<Chart> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<Chart> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<Chart> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<Chart> interface

  // ==================== Description Operations ====================

  /**
   * Sets the description for this chart using patch-based update.
   *
   * @param description the description
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    ChartInfoPatchBuilder patch =
        new ChartInfoPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added description patch");
    return this;
  }

  /**
   * Gets the description for this chart.
   *
   * @return the description, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getDescription() {
    checkNotDirty("read description");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null ? info.getDescription() : null;
  }

  // ==================== Title Operations ====================

  /**
   * Sets the title for this chart using patch-based update.
   *
   * @param title the title
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setTitle(@Nonnull String title) {
    checkNotReadOnly("set title");
    ChartInfoPatchBuilder patch = new ChartInfoPatchBuilder().urn(getUrn()).setTitle(title);
    addPatchMcp(patch.build());
    log.debug("Added title patch");
    return this;
  }

  /**
   * Gets the title for this chart.
   *
   * @return the title, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getTitle() {
    checkNotDirty("read title");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null ? info.getTitle() : null;
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Adds a custom property to this chart.
   *
   * @param key the property key
   * @param value the property value
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.addCustomProperty(key, value);
    log.debug("Added custom property to accumulated builder: {}={}", key, value);
    return this;
  }

  /**
   * Removes a custom property from this chart.
   *
   * @param key the property key to remove
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart removeCustomProperty(@Nonnull String key) {
    checkNotReadOnly("remove custom property");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.removeCustomProperty(key);
    log.debug("Added remove custom property patch: {}", key);
    return this;
  }

  /**
   * Sets custom properties for this chart (replaces all).
   *
   * @param properties map of custom properties
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    ChartInfoPatchBuilder patch =
        new ChartInfoPatchBuilder().urn(getUrn()).setCustomProperties(properties);
    addPatchMcp(patch.build());
    log.debug("Added set custom properties patch with {} properties", properties.size());
    return this;
  }

  // ==================== Lineage Operations ====================

  /**
   * Sets the input datasets for this chart (replaces all existing inputs).
   *
   * <p>This defines the lineage relationship between the chart and the datasets it consumes.
   *
   * @param datasets list of dataset URNs that this chart consumes
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setInputDatasets(@Nonnull List<DatasetUrn> datasets) {
    checkNotReadOnly("set input datasets");
    ChartInfoPatchBuilder patch = new ChartInfoPatchBuilder().urn(getUrn());
    for (DatasetUrn dataset : datasets) {
      patch.addInputEdge(dataset);
    }
    addPatchMcp(patch.build());
    log.debug("Added set input datasets patch with {} datasets", datasets.size());
    return this;
  }

  /**
   * Adds an input dataset to this chart.
   *
   * <p>This creates a lineage relationship indicating the chart consumes data from this dataset.
   *
   * @param dataset the dataset URN to add as input
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart addInputDataset(@Nonnull DatasetUrn dataset) {
    checkNotReadOnly("add input dataset");
    // Get or create accumulated patch builder for chartInfo
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }
    builder.addInputEdge(dataset);
    log.debug("Added input dataset to accumulated patch: {}", dataset);
    return this;
  }

  /**
   * Removes an input dataset from this chart.
   *
   * @param dataset the dataset URN to remove from inputs
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart removeInputDataset(@Nonnull DatasetUrn dataset) {
    checkNotReadOnly("remove input dataset");
    // Get or create accumulated patch builder for chartInfo
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }
    builder.removeInputEdge(dataset);
    log.debug("Added remove input dataset to accumulated patch: {}", dataset);
    return this;
  }

  /**
   * Gets the input datasets for this chart (lazy-loaded).
   *
   * <p>Returns the list of datasets that this chart consumes, based on the inputEdges field in
   * ChartInfo.
   *
   * @return list of input dataset URNs, or empty list if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nonnull
  public List<DatasetUrn> getInputDatasets() {
    checkNotDirty("read input datasets");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    if (info == null || info.getInputEdges() == null) {
      return new ArrayList<>();
    }
    return info.getInputEdges().stream()
        .map(
            edge -> {
              try {
                return DatasetUrn.createFromString(edge.getDestinationUrn().toString());
              } catch (URISyntaxException e) {
                log.warn("Invalid dataset URN in inputEdges: {}", edge.getDestinationUrn(), e);
                return null;
              }
            })
        .filter(urn -> urn != null)
        .collect(Collectors.toList());
  }

  // ==================== Chart-Specific Properties ====================

  /**
   * Sets the external URL for this chart.
   *
   * <p>This URL can be used as an external link on DataHub to allow users to access/view the chart
   * in the source BI tool.
   *
   * @param externalUrl the external URL
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setExternalUrl(@Nonnull String externalUrl) {
    checkNotReadOnly("set external URL");
    ChartInfoPatchBuilder patch =
        new ChartInfoPatchBuilder().urn(getUrn()).setExternalUrl(externalUrl);
    addPatchMcp(patch.build());
    log.debug("Added external URL patch");
    return this;
  }

  /**
   * Gets the external URL for this chart.
   *
   * @return the external URL, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getExternalUrl() {
    checkNotDirty("read external URL");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null && info.getExternalUrl() != null ? info.getExternalUrl().toString() : null;
  }

  /**
   * Sets the chart URL.
   *
   * <p>This is a direct URL to the chart, which may be different from the external URL.
   *
   * @param chartUrl the chart URL
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setChartUrl(@Nonnull String chartUrl) {
    checkNotReadOnly("set chart URL");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.setChartUrl(chartUrl);
    log.debug("Added chart URL to accumulated builder: {}", chartUrl);
    return this;
  }

  /**
   * Gets the chart URL.
   *
   * @return the chart URL, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getChartUrl() {
    checkNotDirty("read chart URL");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null && info.getChartUrl() != null ? info.getChartUrl().toString() : null;
  }

  /**
   * Sets the last refreshed timestamp for this chart.
   *
   * <p>Indicates when the chart data was last refreshed from the source.
   *
   * @param lastRefreshed the timestamp in milliseconds since epoch
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setLastRefreshed(long lastRefreshed) {
    checkNotReadOnly("set last refreshed");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.setLastRefreshed(lastRefreshed);
    log.debug("Added last refreshed to accumulated builder: {}", lastRefreshed);
    return this;
  }

  /**
   * Gets the last refreshed timestamp for this chart.
   *
   * @return the timestamp in milliseconds since epoch, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public Long getLastRefreshed() {
    checkNotDirty("read last refreshed");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null && info.getLastRefreshed() != null ? info.getLastRefreshed() : null;
  }

  /**
   * Sets the chart type.
   *
   * @param chartType the chart type (e.g., "BAR", "LINE", "PIE", "TABLE", "TEXT", "BOXPLOT")
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setChartType(@Nonnull String chartType) {
    checkNotReadOnly("set chart type");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.setType(com.linkedin.chart.ChartType.valueOf(chartType));
    log.debug("Added chart type to accumulated builder: {}", chartType);
    return this;
  }

  /**
   * Gets the chart type.
   *
   * @return the chart type, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getChartType() {
    checkNotDirty("read chart type");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null && info.getType() != null ? info.getType().toString() : null;
  }

  /**
   * Sets the access level for this chart.
   *
   * @param access the access level (e.g., "PUBLIC", "PRIVATE")
   * @return this chart
   * @throws datahub.client.v2.exceptions.ReadOnlyEntityException if entity is read-only
   */
  @RequiresMutable
  @Nonnull
  public Chart setAccess(@Nonnull String access) {
    checkNotReadOnly("set access level");
    ChartInfoPatchBuilder builder = getPatchBuilder("chartInfo", ChartInfoPatchBuilder.class);
    if (builder == null) {
      builder = new ChartInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("chartInfo", builder);
    }

    builder.setAccess(com.linkedin.common.AccessLevel.valueOf(access));
    log.debug("Added access level to accumulated builder: {}", access);
    return this;
  }

  /**
   * Gets the access level for this chart.
   *
   * @return the access level, or null if not set
   * @throws datahub.client.v2.exceptions.PendingMutationsException if entity has pending mutations
   */
  @Nullable
  public String getAccess() {
    checkNotDirty("read access level");
    ChartInfo info = getAspectLazy(ChartInfo.class);
    return info != null && info.getAccess() != null ? info.getAccess().toString() : null;
  }

  /**
   * Helper method to get or create ChartInfo aspect.
   *
   * @return the ChartInfo aspect
   */
  @Nonnull
  private ChartInfo getOrCreateChartInfo() {
    ChartInfo info = getAspectLazy(ChartInfo.class);
    if (info == null) {
      info = new ChartInfo();
      info.setTitle(getChartUrn().getChartIdEntity());
      info.setDescription("");
    }
    return info;
  }

  // ==================== Builder ====================

  public static class Builder {
    private String tool;
    private String id;
    private String title;
    private String description;
    private Map<String, String> customProperties;

    @Nonnull
    public Builder tool(@Nonnull String tool) {
      this.tool = tool;
      return this;
    }

    @Nonnull
    public Builder id(@Nonnull String id) {
      this.id = id;
      return this;
    }

    @Nonnull
    public Builder title(@Nullable String title) {
      this.title = title;
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
     * Sets the URN for this chart by extracting its components.
     *
     * @param urn the chart URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull ChartUrn urn) {
      this.tool = urn.getDashboardToolEntity();
      this.id = urn.getChartIdEntity();
      return this;
    }

    @Nonnull
    public Chart build() {
      if (tool == null || id == null) {
        throw new IllegalArgumentException("tool and id are required");
      }

      ChartUrn urn = new ChartUrn(tool, id);
      Chart chart = new Chart(urn);

      // Cache ChartInfo aspect only if we have required fields
      // Following Stripe's pattern: required fields are only required when creating the aspect
      boolean wantsToCreateInfo = title != null || description != null || customProperties != null;

      if (wantsToCreateInfo) {
        // ChartInfo requires both 'title' and 'description' fields
        if (title == null || description == null) {
          throw new IllegalArgumentException(
              "ChartInfo aspect requires both 'title' and 'description' fields. "
                  + "Provide both .title() and .description() in builder, "
                  + "or don't set customProperties (use addTag/addOwner instead).");
        }

        ChartInfo info = new ChartInfo();
        info.setTitle(title);
        info.setDescription(description);
        if (customProperties != null) {
          info.setCustomProperties(new StringMap(customProperties));
        }

        // Set required lastModified field with current timestamp
        long currentTimeMillis = System.currentTimeMillis();
        AuditStamp auditStamp = new AuditStamp();
        auditStamp.setTime(currentTimeMillis);
        try {
          auditStamp.setActor(Urn.createFromString("urn:li:corpuser:datahub"));
        } catch (URISyntaxException e) {
          throw new RuntimeException("Failed to create actor URN", e);
        }
        ChangeAuditStamps timestamps = new ChangeAuditStamps();
        timestamps.setCreated(auditStamp);
        timestamps.setLastModified(auditStamp);
        info.setLastModified(timestamps);

        String aspectName = chart.getAspectName(ChartInfo.class);
        chart.cache.put(aspectName, info, AspectSource.LOCAL, true);
      }

      return chart;
    }
  }
}
