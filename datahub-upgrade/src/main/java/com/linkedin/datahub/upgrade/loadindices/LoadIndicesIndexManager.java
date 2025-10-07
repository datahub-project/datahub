package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;

/**
 * Manages DataHub Elasticsearch indices during bulk loading operations. Discovers DataHub indices
 * and manages their refresh intervals for optimal bulk loading performance.
 */
@Slf4j
public class LoadIndicesIndexManager {

  private static final String REFRESH_INTERVAL_SETTING = "index.refresh_interval";
  private static final String DISABLED_REFRESH_INTERVAL = "-1";

  private final SearchClientShim<?> searchClient;
  private final IndexConvention indexConvention;
  private final EntityRegistry entityRegistry;
  private final String configuredRefreshInterval;
  private Set<String> managedIndices;
  private boolean refreshDisabled = false;
  private boolean indicesDiscovered = false;

  public LoadIndicesIndexManager(
      SearchClientShim<?> searchClient,
      IndexConvention indexConvention,
      EntityRegistry entityRegistry,
      String configuredRefreshInterval) {
    this.searchClient = searchClient;
    this.indexConvention = indexConvention;
    this.entityRegistry = entityRegistry;
    this.configuredRefreshInterval = configuredRefreshInterval;
    // Delay index discovery until first use
    this.managedIndices = new HashSet<>();
  }

  /**
   * Discovers all DataHub indices that should have refresh intervals managed. This includes entity
   * indices, timeseries indices, and system indices.
   */
  public Set<String> discoverDataHubIndices() throws IOException {
    Set<String> indices = new HashSet<>();

    // Get all existing indices
    GetIndexRequest request = new GetIndexRequest("*");
    GetIndexResponse response = searchClient.getIndex(request, RequestOptions.DEFAULT);
    String[] allIndices = response.getIndices();

    log.info("Found {} total indices in Elasticsearch", allIndices.length);

    // Filter to DataHub indices
    for (String indexName : allIndices) {
      if (isDataHubIndex(indexName)) {
        indices.add(indexName);
        log.debug("Added DataHub index: {}", indexName);
      }
    }

    log.info("Discovered {} DataHub indices for refresh interval management", indices.size());
    return indices;
  }

  /** Disables refresh interval for all DataHub indices. */
  public void disableRefresh() throws IOException {
    if (refreshDisabled) {
      log.warn("Refresh intervals are already disabled");
      return;
    }

    // Discover indices lazily on first use (after BuildIndicesStep has run)
    if (!indicesDiscovered) {
      log.info("Discovering DataHub indices for refresh interval management...");
      this.managedIndices = discoverDataHubIndices();
      this.indicesDiscovered = true;
    }

    log.info("Disabling refresh intervals for {} indices", managedIndices.size());

    for (String indexName : managedIndices) {
      try {
        // Set to disabled (-1)
        setIndexRefreshInterval(indexName, DISABLED_REFRESH_INTERVAL);

        log.debug("Disabled refresh for index: {}", indexName);
      } catch (IOException e) {
        log.error("Failed to disable refresh for index: {}", indexName, e);
        throw e;
      }
    }

    refreshDisabled = true;
    log.info("Successfully disabled refresh intervals for all managed indices");
  }

  /** Restores refresh intervals to configured values for all managed indices. */
  public void restoreRefresh() throws IOException {
    if (!refreshDisabled) {
      log.warn("Refresh intervals are not currently disabled");
      return;
    }

    log.info(
        "Restoring refresh intervals to configured value ({}s) for {} indices",
        configuredRefreshInterval,
        managedIndices.size());

    for (String indexName : managedIndices) {
      try {
        setIndexRefreshInterval(indexName, configuredRefreshInterval + "s");
        log.debug("Restored refresh for index: {} to: {}s", indexName, configuredRefreshInterval);
      } catch (IOException e) {
        log.error("Failed to restore refresh for index: {}", indexName, e);
        throw e;
      }
    }

    refreshDisabled = false;
    log.info("Successfully restored refresh intervals to configured value for all managed indices");
  }

  /** Determines if an index is a DataHub index that should be managed. */
  private boolean isDataHubIndex(String indexName) {
    // Skip system indices
    if (indexName.startsWith(".")) {
      log.debug("Skipping system index: {}", indexName);
      return false;
    }

    // Skip internal Elasticsearch indices
    if (indexName.startsWith("_")) {
      log.debug("Skipping internal Elasticsearch index: {}", indexName);
      return false;
    }

    // Use IndexConvention to determine if this is a DataHub index
    return isDataHubIndexByConvention(indexName);
  }

  /**
   * Uses the IndexConvention and EntityRegistry to determine if an index is a DataHub index. This
   * method follows IndexConvention patterns only - no hardcoded rules or explicit configuration.
   */
  private boolean isDataHubIndexByConvention(String indexName) {

    try {
      // Check if this is an entity index using IndexConvention patterns
      for (String entityName : entityRegistry.getEntitySpecs().keySet()) {
        String entityIndexName = indexConvention.getEntityIndexName(entityName);
        if (indexName.equals(entityIndexName)) {
          log.debug("Identified entity index: {} (entity: {})", indexName, entityName);
          return true;
        }
      }

      // Check if this is a timeseries aspect index using IndexConvention patterns
      for (String entityName : entityRegistry.getEntitySpecs().keySet()) {
        for (AspectSpec aspectSpec : entityRegistry.getEntitySpec(entityName).getAspectSpecs()) {
          String aspectName = aspectSpec.getName();
          String aspectIndexName =
              indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
          if (indexName.equals(aspectIndexName)) {
            log.debug(
                "Identified timeseries aspect index: {} (entity: {}, aspect: {})",
                indexName,
                entityName,
                aspectName);
            return true;
          }
        }
      }

      log.debug("Index {} does not match any IndexConvention patterns", indexName);
      return false;
    } catch (Exception e) {
      log.warn(
          "Error checking index {} against IndexConvention patterns: {}",
          indexName,
          e.getMessage());
      return false;
    }
  }

  /** Gets the current refresh interval for an index. */
  private String getIndexRefreshInterval(String indexName) throws IOException {
    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(indexName)
            .includeDefaults(true)
            .names(REFRESH_INTERVAL_SETTING);

    GetSettingsResponse response = searchClient.getIndexSettings(request, RequestOptions.DEFAULT);
    return response.getSetting(indexName, REFRESH_INTERVAL_SETTING);
  }

  /** Sets the refresh interval for an index. */
  private void setIndexRefreshInterval(String indexName, String interval) throws IOException {
    UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
    Settings settings = Settings.builder().put(REFRESH_INTERVAL_SETTING, interval).build();
    request.settings(settings);

    searchClient.updateIndexSettings(request, RequestOptions.DEFAULT);
  }

  /** Returns true if refresh intervals are currently disabled. */
  public boolean isRefreshDisabled() {
    return refreshDisabled;
  }

  /** Gets the number of managed indices. */
  public int getManagedIndexCount() throws IOException {
    return discoverDataHubIndices().size();
  }
}
