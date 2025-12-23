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
import com.linkedin.mxe.MetadataChangeProposal;
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
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

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
  private final ExecutorService _executorService;

  // Cached set of sample data URNs - loaded once on first access
  private volatile Set<String> _sampleDataUrns;

  public SampleDataService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
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
   * Soft delete all sample data entities asynchronously.
   *
   * @param opContext operation context
   * @return CompletableFuture that completes when all entities are soft-deleted
   */
  public CompletableFuture<Void> softDeleteSampleDataAsync(@Nonnull OperationContext opContext) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            softDeleteOrRestoreSampleData(opContext, true);
          } catch (Exception e) {
            log.error("Failed to soft delete sample data", e);
          }
        },
        _executorService);
  }

  /**
   * Restore all sample data entities asynchronously.
   *
   * @param opContext operation context
   * @return CompletableFuture that completes when all entities are restored
   */
  public CompletableFuture<Void> restoreSampleDataAsync(@Nonnull OperationContext opContext) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            softDeleteOrRestoreSampleData(opContext, false);
          } catch (Exception e) {
            log.error("Failed to restore sample data", e);
          }
        },
        _executorService);
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

  /** Soft delete or restore sample data in batches. */
  private void softDeleteOrRestoreSampleData(
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

    for (List<String> batch : batches) {
      batchNum++;
      try {
        updateStatusForBatch(opContext, batch, softDelete);
        totalProcessed += batch.size();
        log.info(
            "Processed batch {}/{}: {} entities (total: {}/{})",
            batchNum,
            batches.size(),
            batch.size(),
            totalProcessed,
            urns.size());
      } catch (Exception e) {
        log.error(
            "Failed to process batch {} for {} sample data: {}",
            batchNum,
            softDelete ? "soft delete" : "restore",
            e.getMessage());
        // Continue with next batch
      }
    }

    log.info(
        "Completed {} of {} sample data entities",
        softDelete ? "soft delete" : "restore",
        totalProcessed);
  }

  /** Update status aspect for a batch of URNs using EntityClient with retry logic. */
  private void updateStatusForBatch(
      @Nonnull OperationContext opContext, List<String> urnStrs, boolean removed) {
    EntityRegistry entityRegistry = opContext.getEntityRegistry();
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    int numUrns = urnStrs.size();
    int numUrnsProcessed = 0;

    for (String urnStr : urnStrs) {
      try {
        Urn urn = UrnUtils.getUrn(urnStr);
        EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
        if (entitySpec != null && entitySpec.getAspectSpec(Constants.STATUS_ASPECT_NAME) != null) {
          Status status = new Status();
          status.setRemoved(removed);

          MetadataChangeProposal proposal =
              AspectUtils.buildMetadataChangeProposal(urn, Constants.STATUS_ASPECT_NAME, status);
          proposals.add(proposal);
          numUrnsProcessed++;
        } else {
          log.debug(
              "Skipping status update for {}: entity type does not support status aspect", urnStr);
        }
      } catch (Exception e) {
        log.warn("Failed to prepare proposal for {}: {}", urnStr, e.getMessage());
      }
    }

    if (!proposals.isEmpty()) {
      try {
        this.entityClient.batchIngestProposals(opContext, proposals, false);
        log.debug("Batch ingested {} proposals", proposals.size());
      } catch (Exception e) {
        log.error(
            "Failed to update sample data status: ({}) {}",
            e.getClass().getSimpleName(),
            e.getMessage());
      }
    }
    log.info("Updated status for {}/{} sample data urns", numUrnsProcessed, numUrns);
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

            // Filter out platform, user, and group URNs to avoid affecting authentication
            if (urnString != null
                && !urnString.isEmpty()
                && !urnString.startsWith("urn:li:dataPlatform:")
                && !urnString.startsWith("urn:li:corpuser:")
                && !urnString.startsWith("urn:li:corpGroup:")) {
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

  /** Shutdown the executor service gracefully. Called by Spring on bean destruction. */
  @PreDestroy
  public void shutdown() {
    log.info("Shutting down SampleDataService executor");
    _executorService.shutdown();
    try {
      if (!_executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("Executor did not terminate in 30 seconds, forcing shutdown");
        _executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while waiting for executor termination");
      _executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
