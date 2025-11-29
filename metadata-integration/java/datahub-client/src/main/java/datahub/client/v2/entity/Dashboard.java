package datahub.client.v2.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.builder.DashboardInfoPatchBuilder;
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
 * Represents a DataHub Dashboard entity with fluent builder API.
 *
 * <p>A Dashboard represents a collection of visualizations and reports in a BI tool (e.g., Looker,
 * Tableau, PowerBI).
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dashboard dashboard = Dashboard.builder()
 *     .tool("looker")
 *     .id("sales_dashboard")
 *     .title("Sales Performance Dashboard")
 *     .description("Executive dashboard showing key sales metrics")
 *     .build();
 *
 * // Add metadata (creates patches)
 * dashboard.addTag("executive");
 * dashboard.addOwner("urn:li:corpuser:johndoe", OwnershipType.BUSINESS_OWNER);
 * dashboard.addTerm("urn:li:glossaryTerm:SalesMetrics");
 * dashboard.setDomain("urn:li:domain:Sales");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(dashboard);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class Dashboard extends Entity
    implements HasTags<Dashboard>,
        HasGlossaryTerms<Dashboard>,
        HasOwners<Dashboard>,
        HasDomains<Dashboard>,
        HasSubTypes<Dashboard>,
        HasStructuredProperties<Dashboard> {

  private static final String ENTITY_TYPE = "dashboard";

  /**
   * Constructs a new Dashboard entity.
   *
   * @param urn the dashboard URN
   */
  private Dashboard(@Nonnull DashboardUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new Dashboard entity from generic URN.
   *
   * @param urn the dashboard URN (generic type)
   */
  protected Dashboard(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs a Dashboard loaded from server.
   *
   * @param urn the dashboard URN
   * @param aspects current aspects from server
   */
  private Dashboard(
      @Nonnull DashboardUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs a Dashboard loaded from server with generic URN.
   *
   * @param urn the dashboard URN (generic type)
   * @param aspects current aspects from server
   */
  protected Dashboard(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Copy constructor for creating mutable copy via .mutable().
   *
   * @param other the Dashboard to copy from
   */
  protected Dashboard(@Nonnull Dashboard other) {
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
        DashboardInfo.class,
        com.linkedin.dashboard.EditableDashboardProperties.class);
  }

  @Override
  @Nonnull
  public Dashboard mutable() {
    if (!readOnly) {
      return this;
    }
    return new Dashboard(this);
  }

  /**
   * Returns the dashboard URN.
   *
   * @return the dashboard URN
   */
  @Nonnull
  public DashboardUrn getDashboardUrn() {
    return (DashboardUrn) urn;
  }

  /**
   * Creates a new builder for Dashboard.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<Dashboard> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<Dashboard> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<Dashboard> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<Dashboard> interface

  // ==================== Custom Properties Operations ====================

  /**
   * Adds a custom property to this dashboard.
   *
   * @param key the property key
   * @param value the property value
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }

    builder.addCustomProperty(key, value);
    log.debug("Added custom property to accumulated builder: {}={}", key, value);
    return this;
  }

  /**
   * Removes a custom property from this dashboard.
   *
   * @param key the property key to remove
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard removeCustomProperty(@Nonnull String key) {
    checkNotReadOnly("remove custom property");
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }

    builder.removeCustomProperty(key);
    log.debug("Added remove custom property patch: {}", key);
    return this;
  }

  /**
   * Sets custom properties for this dashboard (replaces all).
   *
   * @param properties map of custom properties
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    DashboardInfoPatchBuilder patch =
        new DashboardInfoPatchBuilder().urn(getUrn()).setCustomProperties(properties);
    addPatchMcp(patch.build());
    log.debug("Added set custom properties patch with {} properties", properties.size());
    return this;
  }

  /**
   * Gets the title of this dashboard.
   *
   * @return the title, or null if not set
   */
  @Nullable
  public String getTitle() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    return info != null ? info.getTitle() : null;
  }

  /**
   * Gets the description of this dashboard.
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    return info != null ? info.getDescription() : null;
  }

  // ==================== Input Dataset Lineage Operations ====================

  /**
   * Sets the input datasets for this dashboard (replaces all existing datasets).
   *
   * <p>Input datasets represent the data sources consumed by this dashboard. This creates lineage
   * relationships between the dashboard and its source datasets.
   *
   * @param datasetUrns list of dataset URNs
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard setInputDatasets(@Nonnull List<DatasetUrn> datasetUrns) {
    checkNotReadOnly("set input datasets");
    DashboardInfoPatchBuilder patch = new DashboardInfoPatchBuilder().urn(getUrn());

    // Remove all existing datasets first (if any)
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    if (info != null && info.hasDatasetEdges() && info.getDatasetEdges() != null) {
      for (Edge edge : info.getDatasetEdges()) {
        try {
          DatasetUrn existingUrn = DatasetUrn.createFromUrn(edge.getDestinationUrn());
          patch.removeDatasetEdge(existingUrn);
        } catch (URISyntaxException e) {
          log.warn("Failed to parse dataset URN: {}", edge.getDestinationUrn(), e);
        }
      }
    }

    // Add new datasets
    for (DatasetUrn datasetUrn : datasetUrns) {
      patch.addDatasetEdge(datasetUrn);
    }

    addPatchMcp(patch.build());
    log.debug("Added set input datasets patch with {} datasets", datasetUrns.size());
    return this;
  }

  /**
   * Adds an input dataset to this dashboard.
   *
   * <p>Creates a lineage relationship indicating this dashboard consumes data from the specified
   * dataset.
   *
   * @param datasetUrn the dataset URN
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard addInputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("add input dataset");
    // Get or create accumulated patch builder for dashboardInfo
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }
    builder.addDatasetEdge(datasetUrn);
    log.debug("Added input dataset to accumulated patch: {}", datasetUrn);
    return this;
  }

  /**
   * Adds an input dataset to this dashboard using a string URN.
   *
   * @param datasetUrn the dataset URN as a string
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard addInputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("add input dataset");
    try {
      return addInputDataset(DatasetUrn.createFromString(datasetUrn));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
  }

  /**
   * Removes an input dataset from this dashboard.
   *
   * @param datasetUrn the dataset URN to remove
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard removeInputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("remove input dataset");
    // Get or create accumulated patch builder for dashboardInfo
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }
    builder.removeDatasetEdge(datasetUrn);
    log.debug("Added remove input dataset to accumulated patch: {}", datasetUrn);
    return this;
  }

  /**
   * Removes an input dataset from this dashboard using a string URN.
   *
   * @param datasetUrn the dataset URN to remove as a string
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard removeInputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("remove input dataset");
    try {
      return removeInputDataset(DatasetUrn.createFromString(datasetUrn));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
  }

  /**
   * Gets the input datasets for this dashboard (lazy-loaded).
   *
   * @return list of dataset URNs, or empty list if none set
   */
  @Nonnull
  public List<DatasetUrn> getInputDatasets() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    if (info == null || !info.hasDatasetEdges() || info.getDatasetEdges() == null) {
      return new ArrayList<>();
    }

    return info.getDatasetEdges().stream()
        .map(
            edge -> {
              try {
                return DatasetUrn.createFromUrn(edge.getDestinationUrn());
              } catch (URISyntaxException e) {
                log.warn("Failed to parse dataset URN: {}", edge.getDestinationUrn(), e);
                return null;
              }
            })
        .filter(urn -> urn != null)
        .collect(Collectors.toList());
  }

  // ==================== Chart Relationship Operations ====================

  /**
   * Sets the charts for this dashboard (replaces all existing charts).
   *
   * <p>Charts represent the visualizations embedded in this dashboard. This creates relationships
   * indicating which charts this dashboard contains.
   *
   * @param chartUrns list of chart URNs
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard setCharts(@Nonnull List<ChartUrn> chartUrns) {
    checkNotReadOnly("set charts");
    DashboardInfoPatchBuilder patch = new DashboardInfoPatchBuilder().urn(getUrn());

    // Remove all existing charts first (if any)
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    if (info != null && info.hasChartEdges() && info.getChartEdges() != null) {
      for (Edge edge : info.getChartEdges()) {
        try {
          ChartUrn existingUrn = ChartUrn.createFromUrn(edge.getDestinationUrn());
          patch.removeChartEdge(existingUrn);
        } catch (URISyntaxException e) {
          log.warn("Failed to parse chart URN: {}", edge.getDestinationUrn(), e);
        }
      }
    }

    // Add new charts
    for (ChartUrn chartUrn : chartUrns) {
      patch.addChartEdge(chartUrn);
    }

    addPatchMcp(patch.build());
    log.debug("Added set charts patch with {} charts", chartUrns.size());
    return this;
  }

  /**
   * Adds a chart to this dashboard.
   *
   * <p>Creates a relationship indicating this dashboard contains the specified chart.
   *
   * @param chartUrn the chart URN
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard addChart(@Nonnull ChartUrn chartUrn) {
    checkNotReadOnly("add chart");
    // Get or create accumulated patch builder for dashboardInfo
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }
    builder.addChartEdge(chartUrn);
    log.debug("Added chart to accumulated patch: {}", chartUrn);
    return this;
  }

  /**
   * Adds a chart to this dashboard using a string URN.
   *
   * @param chartUrn the chart URN as a string
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard addChart(@Nonnull String chartUrn) {
    checkNotReadOnly("add chart");
    try {
      return addChart(ChartUrn.createFromString(chartUrn));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid chart URN: " + chartUrn, e);
    }
  }

  /**
   * Removes a chart from this dashboard.
   *
   * @param chartUrn the chart URN to remove
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard removeChart(@Nonnull ChartUrn chartUrn) {
    checkNotReadOnly("remove chart");
    // Get or create accumulated patch builder for dashboardInfo
    DashboardInfoPatchBuilder builder =
        getPatchBuilder("dashboardInfo", DashboardInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DashboardInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dashboardInfo", builder);
    }
    builder.removeChartEdge(chartUrn);
    log.debug("Added remove chart to accumulated patch: {}", chartUrn);
    return this;
  }

  /**
   * Removes a chart from this dashboard using a string URN.
   *
   * @param chartUrn the chart URN to remove as a string
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard removeChart(@Nonnull String chartUrn) {
    checkNotReadOnly("remove chart");
    try {
      return removeChart(ChartUrn.createFromString(chartUrn));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid chart URN: " + chartUrn, e);
    }
  }

  /**
   * Gets the charts for this dashboard (lazy-loaded).
   *
   * @return list of chart URNs, or empty list if none set
   */
  @Nonnull
  public List<ChartUrn> getCharts() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    if (info == null || !info.hasChartEdges() || info.getChartEdges() == null) {
      return new ArrayList<>();
    }

    return info.getChartEdges().stream()
        .map(
            edge -> {
              try {
                return ChartUrn.createFromUrn(edge.getDestinationUrn());
              } catch (URISyntaxException e) {
                log.warn("Failed to parse chart URN: {}", edge.getDestinationUrn(), e);
                return null;
              }
            })
        .filter(urn -> urn != null)
        .collect(Collectors.toList());
  }

  // ==================== Dashboard-Specific Properties ====================

  /**
   * Sets the dashboard URL (direct link to the dashboard).
   *
   * <p>This URL can be used as an external link in DataHub to allow users to access the dashboard
   * in its native BI tool.
   *
   * @param dashboardUrl the dashboard URL
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard setDashboardUrl(@Nonnull String dashboardUrl) {
    checkNotReadOnly("set dashboard URL");
    DashboardInfo info = getOrCreateAspect(DashboardInfo.class);
    info.setDashboardUrl(new com.linkedin.common.url.Url(dashboardUrl));
    log.debug("Set dashboard URL: {}", dashboardUrl);
    return this;
  }

  /**
   * Gets the dashboard URL.
   *
   * @return the dashboard URL, or null if not set
   */
  @Nullable
  public String getDashboardUrl() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    return info != null && info.getDashboardUrl() != null
        ? info.getDashboardUrl().toString()
        : null;
  }

  /**
   * Sets the last refreshed timestamp (when dashboard data was last updated).
   *
   * <p>This indicates when the dashboard's underlying data was last refreshed or recomputed.
   *
   * @param lastRefreshedMillis the last refresh timestamp in milliseconds since epoch
   * @return this dashboard
   */
  @RequiresMutable
  @Nonnull
  public Dashboard setLastRefreshed(long lastRefreshedMillis) {
    checkNotReadOnly("set last refreshed");
    DashboardInfo info = getOrCreateAspect(DashboardInfo.class);
    info.setLastRefreshed(lastRefreshedMillis);
    log.debug("Set last refreshed: {}", lastRefreshedMillis);
    return this;
  }

  /**
   * Gets the last refreshed timestamp.
   *
   * @return the last refresh timestamp in milliseconds since epoch, or null if not set
   */
  @Nullable
  public Long getLastRefreshed() {
    DashboardInfo info = getAspectLazy(DashboardInfo.class);
    return info != null && info.hasLastRefreshed() ? info.getLastRefreshed() : null;
  }

  // ==================== Builder ====================

  /** Builder for creating Dashboard entities with a fluent API. */
  public static class Builder {
    private String tool;
    private String id;
    private String title;
    private String description;
    private Map<String, String> customProperties;

    /**
     * Sets the dashboard tool (required).
     *
     * @param tool the BI tool name (e.g., "looker", "tableau", "powerbi")
     * @return this builder
     */
    @Nonnull
    public Builder tool(@Nonnull String tool) {
      this.tool = tool;
      return this;
    }

    /**
     * Sets the dashboard ID (required).
     *
     * @param id the dashboard identifier within the tool
     * @return this builder
     */
    @Nonnull
    public Builder id(@Nonnull String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the dashboard title.
     *
     * @param title the dashboard title
     * @return this builder
     */
    @Nonnull
    public Builder title(@Nullable String title) {
      this.title = title;
      return this;
    }

    /**
     * Sets the dashboard description.
     *
     * @param description the dashboard description
     * @return this builder
     */
    @Nonnull
    public Builder description(@Nullable String description) {
      this.description = description;
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
     * Sets the URN for this dashboard by extracting its components.
     *
     * @param urn the dashboard URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull DashboardUrn urn) {
      this.tool = urn.getDashboardToolEntity();
      this.id = urn.getDashboardIdEntity();
      return this;
    }

    /**
     * Builds the Dashboard entity.
     *
     * <p>Note: Properties set in the builder are cached as aspects, not converted to patches. Use
     * the setter methods on the Dashboard after building if you want patch-based updates.
     *
     * @return the Dashboard entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public Dashboard build() {
      if (tool == null || id == null) {
        throw new IllegalArgumentException("tool and id are required");
      }

      DashboardUrn urn = new DashboardUrn(tool, id);
      Dashboard dashboard = new Dashboard(urn);

      // Cache DashboardInfo aspect only if we have required fields
      // Following Stripe's pattern: required fields are only required when creating the aspect
      boolean wantsToCreateInfo = title != null || description != null || customProperties != null;

      if (wantsToCreateInfo) {
        // DashboardInfo requires both 'title' and 'description' fields
        if (title == null || description == null) {
          throw new IllegalArgumentException(
              "DashboardInfo aspect requires both 'title' and 'description' fields. "
                  + "Provide both .title() and .description() in builder, "
                  + "or don't set customProperties (use addTag/addOwner instead).");
        }

        DashboardInfo info = new DashboardInfo();
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

        String aspectName = dashboard.getAspectName(DashboardInfo.class);
        dashboard.cache.put(aspectName, info, AspectSource.LOCAL, true);
      }

      return dashboard;
    }
  }
}
