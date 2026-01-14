package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import com.linkedin.settings.global.SampleDataSettings;
import com.linkedin.settings.global.SampleDataStatus;
import io.datahubproject.metadata.context.OperationContext;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Service to manage sample data visibility in the platform.
 *
 * <p>Provides methods to soft-delete and restore sample data entities, as well as check if sample
 * data is currently enabled via GlobalSettings.
 */
@Slf4j
public class SampleDataService {

  private static final String SAMPLE_DATA_MCP_PATH = "boot/sample_data_mcp.json";
  private static final int BATCH_SIZE = 500;

  private final SystemEntityClient entityClient;
  private final EntitySearchService searchService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final ExecutorService _executorService;

  // Optional - only available in free trial instances for statistics generation
  @Nullable private final Object statisticsGenerator;

  // Cached set of sample data URNs - loaded once on first access
  private volatile Set<String> _sampleDataUrns;

  // Cached set of sample data URNs that support status aspect - populated during first operation
  private volatile Set<String> _sampleDataUrnsWithStatus;

  // Operation tracking fields
  private volatile CompletableFuture<Void> _currentOperation;
  private final ReadWriteLock _operationLock = new ReentrantReadWriteLock();
  private volatile OperationContext _lastOperationContext;

  public SampleDataService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull EntitySearchService searchService,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable Object statisticsGenerator) {
    this.entityClient = entityClient;
    this.searchService = searchService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.statisticsGenerator = statisticsGenerator;
    _executorService = Executors.newSingleThreadExecutor();
  }

  /**
   * Check if sample data is enabled in GlobalSettings.
   *
   * @param opContext operation context
   * @return true if enabled, false otherwise
   */
  public boolean isSampleDataEnabled(@Nonnull OperationContext opContext) {
    try {
      EntityResponse response =
          this.entityClient.getV2(
              opContext,
              Constants.GLOBAL_SETTINGS_ENTITY_NAME,
              Constants.GLOBAL_SETTINGS_URN,
              Set.of(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME));

      if (response != null
          && response.getAspects().containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        GlobalSettingsInfo settings =
            new GlobalSettingsInfo(
                response
                    .getAspects()
                    .get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)
                    .getValue()
                    .data());

        if (settings.getVisual() != null && settings.getVisual().getSampleDataSettings() != null) {
          return settings.getVisual().getSampleDataSettings().getSampleDataStatus()
              == SampleDataStatus.ENABLED;
        }
      }
      // Default to enabled if setting doesn't exist
      return true;
    } catch (Exception e) {
      log.warn(
          "Failed to check sample data enabled status, defaulting to true: {}", e.getMessage());
      return true;
    }
  }

  /**
   * Update sample data status setting in GlobalSettings.
   *
   * @param opContext operation context
   * @param enabled new enabled status (true for ENABLED, false for DISABLED)
   */
  public void updateSampleDataSetting(@Nonnull OperationContext opContext, boolean enabled) {
    try {
      // Get existing settings
      EntityResponse response =
          this.entityClient.getV2(
              opContext,
              Constants.GLOBAL_SETTINGS_ENTITY_NAME,
              Constants.GLOBAL_SETTINGS_URN,
              Set.of(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME));

      GlobalSettingsInfo settings;
      if (response != null
          && response.getAspects().containsKey(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)) {
        settings =
            new GlobalSettingsInfo(
                response
                    .getAspects()
                    .get(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME)
                    .getValue()
                    .data());
      } else {
        settings = new GlobalSettingsInfo();
      }

      GlobalVisualSettings visualSettings =
          settings.getVisual() != null ? settings.getVisual() : new GlobalVisualSettings();

      SampleDataSettings sampleDataSettings =
          visualSettings.getSampleDataSettings() != null
              ? visualSettings.getSampleDataSettings()
              : new SampleDataSettings();
      sampleDataSettings.setSampleDataStatus(
          enabled ? SampleDataStatus.ENABLED : SampleDataStatus.DISABLED);

      visualSettings.setSampleDataSettings(sampleDataSettings);
      settings.setVisual(visualSettings);

      // Save
      MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(
              Constants.GLOBAL_SETTINGS_URN, Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME, settings);
      this.entityClient.ingestProposal(opContext, proposal, false);

      log.info("Updated sample data setting to status={}", enabled ? "ENABLED" : "DISABLED");
    } catch (Exception e) {
      throw new RuntimeException("Failed to update sample data setting", e);
    }
  }

  /**
   * Soft delete all sample data entities asynchronously with operation status tracking.
   *
   * @param opContext operation context
   * @return CompletableFuture that completes when all entities are soft-deleted
   * @throws IllegalStateException if an operation is already in progress
   */
  public CompletableFuture<Void> softDeleteSampleDataAsync(@Nonnull OperationContext opContext) {
    return startOperationAsync(opContext, true);
  }

  /**
   * Restore all sample data entities asynchronously with operation status tracking.
   *
   * @param opContext operation context
   * @return CompletableFuture that completes when all entities are restored
   * @throws IllegalStateException if an operation is already in progress
   */
  public CompletableFuture<Void> restoreSampleDataAsync(@Nonnull OperationContext opContext) {
    return startOperationAsync(opContext, false);
  }

  /**
   * Start a new operation (soft delete or restore) with proper concurrency control.
   *
   * @param opContext operation context
   * @param isSoftDelete true for soft delete, false for restore
   * @return CompletableFuture for the operation
   * @throws IllegalStateException if an operation is already in progress or ES is still reindexing
   */
  private CompletableFuture<Void> startOperationAsync(
      @Nonnull OperationContext opContext, boolean isSoftDelete) {
    _operationLock.writeLock().lock();
    try {
      // Check if operation already in progress (MCP processing)
      log.info(
          "Checking if operation in progress: _currentOperation={}, isDone={}",
          _currentOperation != null,
          _currentOperation != null ? _currentOperation.isDone() : "N/A");
      if (_currentOperation != null && !_currentOperation.isDone()) {
        throw new IllegalStateException(
            "Sample data operation already in progress. Please wait for it to complete.");
      }

      // Check if ES is still reindexing from previous operation
      // Skip this check on first operation (when _currentOperation is null) to allow toggling
      // immediately after bootstrap without waiting for ES to finish indexing bootstrap data
      if (_currentOperation != null) {
        // We expect ES to be in the opposite state of what we're about to do
        boolean expectedCurrentlyRemoved = !isSoftDelete;
        log.info(
            "Checking ES sync: isSoftDelete={}, expectedCurrentlyRemoved={}",
            isSoftDelete,
            expectedCurrentlyRemoved);
        boolean esInSync = isEsInSyncWithExpectedState(opContext, expectedCurrentlyRemoved);
        log.info("ES sync check returned: {}", esInSync);
        if (!esInSync) {
          throw new IllegalStateException(
              "Previous operation still reindexing in Elasticsearch. Please wait for the operation to complete and try again.");
        }
      } else {
        log.info(
            "First sample data operation since GMS started - skipping ES sync check and proceeding");
      }

      // Store context for potential shutdown
      _lastOperationContext = opContext;

      final String operationType = isSoftDelete ? "soft-delete" : "restore";

      // Start new operation
      _currentOperation =
          CompletableFuture.runAsync(
              () -> {
                try {
                  // Handle statistics before sample data operations
                  if (isSoftDelete) {
                    // When toggling OFF: delete statistics first
                    deleteOrderDetailsStatistics(opContext);
                  }

                  // Process all sample data entities
                  OperationResult result =
                      softDeleteOrRestoreSampleDataWithProgress(opContext, isSoftDelete);

                  if (!result.failedUrns.isEmpty()) {
                    log.warn(
                        "Sample data {} operation completed with {} failures",
                        operationType,
                        result.failedUrns.size());
                  }

                  // Handle statistics after sample data operations
                  if (!isSoftDelete) {
                    // When toggling ON: regenerate statistics after restoring sample data
                    regenerateOrderDetailsStatistics(opContext);
                  }
                } catch (Exception e) {
                  log.error("Failed to {} sample data", operationType, e);
                }
              },
              _executorService);

      return _currentOperation;
    } finally {
      _operationLock.writeLock().unlock();
    }
  }

  /**
   * Check if Elasticsearch is in sync with the expected state for sample data entities.
   *
   * <p>Uses a representative sample data entity (order_details) to check if ES has finished
   * reindexing. This method searches using the same search path as the UI (searchAcrossEntities),
   * which automatically filters out removed entities. If the entity is found, it means ES has
   * indexed it as active (removed=false). If not found, it means ES has indexed it as removed
   * (removed=true) or hasn't indexed it yet.
   *
   * @param opContext operation context
   * @param expectedRemoved true if we expect entities to be soft-deleted (removed=true), false if
   *     active (removed=false)
   * @return true if ES state matches expected state, false if still reindexing
   */
  private boolean isEsInSyncWithExpectedState(
      @Nonnull OperationContext opContext, boolean expectedRemoved) {
    try {
      // Use order_details as a representative sample data entity
      // This is a key entity that always exists in sample data
      String searchQuery = "order_details";

      log.info(
          "ES sync check: searching for '{}' to verify sample data state (expecting removed={})",
          searchQuery,
          expectedRemoved);

      // Search using the same method as the UI - this automatically filters removed entities
      SearchResult result =
          searchService.search(
              opContext,
              List.of("dataset"), // order_details is a dataset
              searchQuery,
              null, // no additional filters
              null, // no sort
              0, // from
              10); // size - just need to know if any results exist

      boolean foundInSearch = result.getNumEntities() > 0;

      log.info(
          "ES sync check: search for '{}' returned {} results (found={})",
          searchQuery,
          result.getNumEntities(),
          foundInSearch);

      // Determine if in sync based on search results
      boolean isInSync;
      if (expectedRemoved) {
        // Expecting removed=true: should NOT be found in search (UI filters removed entities)
        isInSync = !foundInSearch;
        log.info(
            "ES sync check: expecting removed=true, found={}, isInSync={}",
            foundInSearch,
            isInSync);
      } else {
        // Expecting removed=false: SHOULD be found in search
        isInSync = foundInSearch;
        log.info(
            "ES sync check: expecting removed=false, found={}, isInSync={}",
            foundInSearch,
            isInSync);
      }

      return isInSync;
    } catch (Exception e) {
      log.warn("ES sync check failed: {}", e.getMessage(), e);
      // If we can't check, assume not in sync to be safe
      return false;
    }
  }

  /** Result of soft delete/restore operation. */
  private static class OperationResult {
    int processedCount;
    List<String> failedUrns;

    OperationResult(int processedCount, List<String> failedUrns) {
      this.processedCount = processedCount;
      this.failedUrns = failedUrns;
    }
  }

  /**
   * Get the set of all sample data URNs.
   *
   * @return set of URN strings
   */
  @Nonnull
  public Set<String> getSampleDataUrns() {
    if (_sampleDataUrns == null) {
      synchronized (this) {
        if (_sampleDataUrns == null) {
          _sampleDataUrns = loadSampleDataUrns();
        }
      }
    }
    return _sampleDataUrns;
  }

  /**
   * Get the set of sample data URNs that support the status aspect.
   *
   * <p>This is computed once during the first operation and cached. If GMS restarts before an
   * operation runs, this computes it on-demand by checking entity specs.
   *
   * @param opContext operation context
   * @return set of URN strings that support status
   */
  @Nonnull
  private Set<String> getSampleDataUrnsWithStatus(@Nonnull OperationContext opContext) {
    if (_sampleDataUrnsWithStatus == null) {
      synchronized (this) {
        if (_sampleDataUrnsWithStatus == null) {
          _sampleDataUrnsWithStatus = computeUrnsWithStatus(opContext);
        }
      }
    }
    return _sampleDataUrnsWithStatus;
  }

  /**
   * Compute which sample data URNs support the status aspect.
   *
   * @param opContext operation context
   * @return set of URN strings that support status
   */
  @Nonnull
  private Set<String> computeUrnsWithStatus(@Nonnull OperationContext opContext) {
    Set<String> urnsWithStatus = new HashSet<>();
    EntityRegistry entityRegistry = opContext.getEntityRegistry();

    for (String urnStr : getSampleDataUrns()) {
      try {
        Urn urn = UrnUtils.getUrn(urnStr);
        EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
        if (entitySpec != null && entitySpec.getAspectSpec(Constants.STATUS_ASPECT_NAME) != null) {
          urnsWithStatus.add(urnStr);
        }
      } catch (Exception e) {
        log.debug("Failed to check status support for {}: {}", urnStr, e.getMessage());
      }
    }

    log.info(
        "Computed URNs with status support: {} out of {} sample data entities",
        urnsWithStatus.size(),
        getSampleDataUrns().size());

    return urnsWithStatus;
  }

  /**
   * Soft delete or restore sample data in batches with progress tracking.
   *
   * @param opContext operation context
   * @param softDelete true for soft delete, false for restore
   * @return OperationResult with processed count and failed URNs
   */
  private OperationResult softDeleteOrRestoreSampleDataWithProgress(
      @Nonnull OperationContext opContext, boolean softDelete) {
    Set<String> urns = getSampleDataUrns();
    log.info(
        "Starting to {} {} sample data entities",
        softDelete ? "soft delete" : "restore",
        urns.size());

    List<String> urnList = new ArrayList<>(urns);
    List<List<String>> batches = Lists.partition(urnList, BATCH_SIZE);

    int batchNum = 0;
    int totalProcessed = 0;
    List<String> failedUrns = new ArrayList<>();
    Set<String> urnsWithStatus = new HashSet<>();

    for (List<String> batch : batches) {
      batchNum++;
      try {
        BatchResult batchResult = updateStatusForBatch(opContext, batch, softDelete);
        totalProcessed += batchResult.processedCount;
        failedUrns.addAll(batchResult.failedUrns);
        urnsWithStatus.addAll(batchResult.urnsWithStatus);

        log.info(
            "Processed batch {}/{}: {}/{} entities succeeded (total: {}/{})",
            batchNum,
            batches.size(),
            batchResult.processedCount,
            batch.size(),
            totalProcessed,
            urns.size());
      } catch (Exception e) {
        log.error(
            "Failed to process batch {} for {} sample data: {}",
            batchNum,
            softDelete ? "soft delete" : "restore",
            e.getMessage());
        // Add all batch URNs to failed list
        failedUrns.addAll(batch);
      }
    }

    // Cache URNs that support status for future ES sync checks
    _sampleDataUrnsWithStatus = urnsWithStatus;
    log.info(
        "{} out of {} sample data entities support status aspect",
        urnsWithStatus.size(),
        urns.size());

    log.info(
        "Completed {} of {} sample data entities: {} succeeded, {} failed",
        softDelete ? "soft delete" : "restore",
        urns.size(),
        totalProcessed,
        failedUrns.size());

    return new OperationResult(totalProcessed, failedUrns);
  }

  /**
   * Update status aspect for a batch of URNs with failure tracking.
   *
   * @param opContext operation context
   * @param urnStrs list of URN strings to update
   * @param removed true to soft delete, false to restore
   * @return BatchResult with success and failure counts
   */
  private BatchResult updateStatusForBatch(
      @Nonnull OperationContext opContext, List<String> urnStrs, boolean removed) {
    EntityRegistry entityRegistry = opContext.getEntityRegistry();
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    List<String> proposalUrns = new ArrayList<>();
    List<String> failedUrns = new ArrayList<>();
    int numUrns = urnStrs.size();

    // Build proposals for entities that support status aspect
    for (String urnStr : urnStrs) {
      try {
        Urn urn = UrnUtils.getUrn(urnStr);
        EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
        if (entitySpec != null && entitySpec.getAspectSpec(Constants.STATUS_ASPECT_NAME) != null) {
          Status status = new Status();
          status.setRemoved(removed);

          MetadataChangeProposal proposal =
              AspectUtils.buildMetadataChangeProposal(urn, Constants.STATUS_ASPECT_NAME, status);

          // Mark this entity as sample data in systemMetadata so it can be filtered in ES
          SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
          if (!systemMetadata.hasProperties()) {
            systemMetadata.setProperties(new com.linkedin.data.template.StringMap());
          }
          systemMetadata.getProperties().put("sampleData", "true");
          proposal.setSystemMetadata(systemMetadata);

          proposals.add(proposal);
          proposalUrns.add(urnStr);
        } else {
          if (entitySpec == null) {
            log.info(
                "Skipping {}: entity type '{}' not found in registry", urnStr, urn.getEntityType());
          } else {
            log.info(
                "Skipping {}: entity type '{}' does not support status aspect",
                urnStr,
                urn.getEntityType());
          }
          failedUrns.add(urnStr);
        }
      } catch (Exception e) {
        log.warn("Failed to prepare proposal for {}: {}", urnStr, e.getMessage());
        failedUrns.add(urnStr);
      }
    }

    if (!failedUrns.isEmpty()) {
      log.info(
          "Skipped {} entities that don't support status aspect in this batch", failedUrns.size());
    }

    // Ingest proposals
    int processedCount = 0;
    if (!proposals.isEmpty()) {
      try {
        this.entityClient.batchIngestProposals(opContext, proposals, false);
        processedCount = proposals.size();
        log.debug("Batch ingested {} proposals", proposals.size());
      } catch (Exception e) {
        log.error("Failed to ingest batch: ({}) {}", e.getClass().getSimpleName(), e.getMessage());
        // All proposals in this batch failed
        failedUrns.addAll(proposalUrns);
      }
    }

    log.debug("Updated status for {}/{} sample data urns in batch", processedCount, numUrns);
    return new BatchResult(processedCount, failedUrns, proposalUrns);
  }

  /** Result of processing a batch of entities. */
  private static class BatchResult {
    int processedCount;
    List<String> failedUrns;
    List<String> urnsWithStatus;

    BatchResult(int processedCount, List<String> failedUrns, List<String> urnsWithStatus) {
      this.processedCount = processedCount;
      this.failedUrns = failedUrns;
      this.urnsWithStatus = urnsWithStatus;
    }
  }

  /**
   * Load sample data URNs by parsing the MCP JSON file directly.
   *
   * <p>Filters out platform, user, and group URNs to avoid soft-deleting critical entities like the
   * admin user.
   */
  @Nonnull
  private Set<String> loadSampleDataUrns() {
    Set<String> urns = new HashSet<>();
    try {
      ObjectMapper mapper = new ObjectMapper();
      InputStream inputStream =
          getClass().getClassLoader().getResourceAsStream(SAMPLE_DATA_MCP_PATH);
      if (inputStream == null) {
        log.error("Could not find sample data MCP file: {}", SAMPLE_DATA_MCP_PATH);
        return urns;
      }

      JsonNode mcpArray = mapper.readTree(inputStream);

      if (mcpArray.isArray()) {
        for (JsonNode mcpNode : mcpArray) {
          JsonNode entityUrnNode = mcpNode.get("entityUrn");
          if (entityUrnNode != null && entityUrnNode.isTextual()) {
            String urnString = entityUrnNode.asText();

            // No filtering needed - only entities with systemMetadata.properties.sampleData=true
            // are in the sample_data_mcp.json file. This includes sample users/groups but
            // excludes real users (admin, datahub) and real groups.
            if (urnString != null && !urnString.isEmpty()) {
              urns.add(urnString);
            }
          }
        }
      }

      log.info("Loaded {} sample data URNs from {}", urns.size(), SAMPLE_DATA_MCP_PATH);
    } catch (Exception e) {
      log.error(
          "Failed to load sample data URNs from {}: {}", SAMPLE_DATA_MCP_PATH, e.getMessage());
    }
    return urns;
  }

  /**
   * Regenerate statistics for the order_details dataset.
   *
   * <p>Uses reflection to call OrderDetailsStatisticsGenerator to avoid compile-time dependency on
   * the factories module. The generator is optionally injected at construction time.
   *
   * @param opContext operation context
   */
  /**
   * Delete all statistics aspects for the order_details dataset.
   *
   * <p>This hard-deletes both datasetProfile and datasetUsageStatistics timeseries aspects. Called
   * when toggling sample data OFF.
   *
   * @param opContext operation context
   */
  private void deleteOrderDetailsStatistics(@Nonnull OperationContext opContext) {
    if (statisticsGenerator == null) {
      log.info(
          "Statistics generator not available (not a free trial instance). Skipping statistics deletion.");
      return;
    }

    try {
      log.info(
          "Deleting order_details statistics aspects (datasetProfile and datasetUsageStatistics)");

      // Get the order_details dataset URN using reflection to access the static field
      String orderDetailsUrnStr =
          (String) statisticsGenerator.getClass().getField("ORDER_DETAILS_DATASET_URN").get(null);

      // Build filter to match all statistics for this specific URN
      Criterion urnCriterion =
          new Criterion()
              .setField("urn")
              .setValue(orderDetailsUrnStr)
              .setCondition(Condition.EQUAL);
      CriterionArray andCriteria = new CriterionArray();
      andCriteria.add(urnCriterion);
      ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      conjunction.setAnd(andCriteria);
      ConjunctiveCriterionArray orClauses = new ConjunctiveCriterionArray();
      orClauses.add(conjunction);
      Filter filter = new Filter().setOr(orClauses);

      // Delete datasetProfile timeseries aspects
      timeseriesAspectService.deleteAspectValues(
          opContext, Constants.DATASET_ENTITY_NAME, Constants.DATASET_PROFILE_ASPECT_NAME, filter);

      // Delete datasetUsageStatistics timeseries aspects
      timeseriesAspectService.deleteAspectValues(
          opContext,
          Constants.DATASET_ENTITY_NAME,
          Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME,
          filter);

      log.info("Successfully deleted order_details statistics aspects");
    } catch (Exception e) {
      // Don't fail the soft-delete operation if statistics deletion fails
      log.error("Failed to delete order_details statistics: {}", e.getMessage(), e);
    }
  }

  /**
   * Regenerate all statistics aspects for the order_details dataset.
   *
   * <p>This generates both datasetProfile and datasetUsageStatistics timeseries aspects. Called
   * when toggling sample data ON.
   *
   * @param opContext operation context
   */
  private void regenerateOrderDetailsStatistics(@Nonnull OperationContext opContext) {
    if (statisticsGenerator == null) {
      log.info(
          "Statistics generator not available (not a free trial instance). Skipping statistics regeneration.");
      return;
    }

    try {
      log.info("Regenerating order_details statistics after sample data restore");

      // Calculate date range: 30 days past + today + 30 days future = 61 total days
      final DateTime now = DateTime.now(DateTimeZone.UTC);
      final DateTime startOfToday = now.withTimeAtStartOfDay();
      final int historicalDays = 30;
      final int futureDays = 30;
      final DateTime startDate = startOfToday.minusDays(historicalDays);
      final int totalDays = historicalDays + 1 + futureDays;

      // Use reflection to call: statisticsGenerator.generateHistoricalStatistics(opContext,
      // startDate.getMillis(), totalDays)
      java.lang.reflect.Method method =
          statisticsGenerator
              .getClass()
              .getMethod(
                  "generateHistoricalStatistics", OperationContext.class, long.class, int.class);
      method.invoke(statisticsGenerator, opContext, startDate.getMillis(), totalDays);

      log.info("Successfully regenerated {} days of order_details statistics", totalDays);
    } catch (Exception e) {
      // Don't fail the restore operation if statistics generation fails
      log.error("Failed to regenerate order_details statistics: {}", e.getMessage(), e);
    }
  }

  /**
   * Shutdown the executor service gracefully.
   *
   * <p>Marks any in-progress operation as FAILED and attempts graceful shutdown. Called by Spring
   * on bean destruction.
   */
  @PreDestroy
  public void shutdown() {
    log.info("Shutting down SampleDataService executor");

    // Mark any in-progress operation as interrupted
    _operationLock.writeLock().lock();
    try {
      if (_currentOperation != null && !_currentOperation.isDone()) {
        log.warn("Operation in progress during shutdown, will be interrupted");
      }
    } finally {
      _operationLock.writeLock().unlock();
    }

    // Graceful shutdown with increased timeout
    _executorService.shutdown();
    try {
      if (!_executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        log.warn("Executor did not terminate in 60 seconds, forcing shutdown");
        _executorService.shutdownNow();
        if (!_executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          log.error("Executor did not terminate after forced shutdown");
        }
      }
    } catch (InterruptedException e) {
      log.error("Interrupted while waiting for executor shutdown", e);
      _executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
