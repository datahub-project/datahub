package com.linkedin.datahub.upgrade.system.sampledata;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.SampleDataVersionInfo;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.McpJsonUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platformresource.PlatformResourceInfo;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import com.linkedin.settings.global.SampleDataSettings;
import com.linkedin.settings.global.SampleDataStatus;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

/**
 * Upgrade step to ingest sample data for free trial instances.
 *
 * <p>The JSON file format is a JSON array of standard MCPs.
 */
@Slf4j
public class IngestSampleDataStep implements UpgradeStep {

  private static final String UPGRADE_ID_PREFIX = "IngestSampleData";
  private static final String DEFAULT_FILE_PATH = "boot/sample_data_mcp.json";
  private static final int MAX_PARSE_ERRORS_TO_LOG = 10;
  private static final String SAMPLE_DATA_PROPERTY_KEY = "sampleData";
  private static final String SAMPLE_DATA_PROPERTY_VALUE = "true";

  // Sample data version tracking
  private static final String SAMPLE_DATA_VERSION_URN_STRING =
      "urn:li:platformResource:datahub-sample-data-version";
  private static final String CURRENT_VERSION =
      "a52b365c5b91"; // Hash-based version (embedded URNs in text fields prefixed)

  // Probe URNs for detecting unprefixed sample data (known entities from version 001)
  // We check multiple URNs to increase reliability - if ANY exists, we have unprefixed data
  private static final String[] UNPREFIXED_PROBE_URNS = {
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)",
    "urn:li:dashboard:(looker,dashboards.53)",
    "urn:li:chart:(looker,dashboard_elements.221)"
  };

  // Hard-delete timeout configuration
  private static final long HARD_DELETE_TIMEOUT_MINUTES = 5;

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final boolean enabled;
  private final boolean reprocessEnabled;
  private final int batchSize;
  private final String filePath;
  private final String upgradeId;
  private final Urn upgradeIdUrn;

  // Track async hard-delete future to ensure completion before shutdown
  // Volatile ensures visibility across threads (main thread sets, shutdown may read)
  private volatile CompletableFuture<Void> hardDeleteFuture;

  // Dedicated executor for hard-delete to ensure proper lifecycle management
  private final ExecutorService hardDeleteExecutor;

  public IngestSampleDataStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize) {
    this(systemOpContext, entityService, enabled, reprocessEnabled, batchSize, DEFAULT_FILE_PATH);
  }

  public IngestSampleDataStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize,
      String filePath) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.enabled = enabled;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
    this.filePath = filePath;
    this.upgradeId = UPGRADE_ID_PREFIX + "-" + computeFileHash(filePath);
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(this.upgradeId);
    // Dedicated single-thread executor for hard-delete operations
    // This ensures predictable behavior and proper shutdown handling
    this.hardDeleteExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "sample-data-hard-delete");
              t.setDaemon(true); // Allow JVM to exit if this is the only thread
              return t;
            });
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final ObjectMapper mapper = context.opContext().getObjectMapper();

      try {
        log.info("Starting sample data migration/ingestion from: {}", filePath);

        // Step 1: Fetch installed version
        String installedVersion = fetchInstalledVersion(context.opContext());

        // Skip if already on current version
        if (CURRENT_VERSION.equals(installedVersion)) {
          log.info("Sample data already on version {}, skipping migration", CURRENT_VERSION);
          // Mark upgrade as complete to prevent re-running on every restart
          BootstrapStep.setUpgradeResult(context.opContext(), upgradeIdUrn, entityService);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
        }

        // Step 2: Check toggle state
        boolean toggleOff = isToggleOff(context.opContext());

        // Step 3: Calculate migration diff
        ManifestDiff diff =
            calculateMigrationDiff(context.opContext(), installedVersion, CURRENT_VERSION);

        // Step 4: Soft-delete + async hard-delete old entities
        if (!diff.getHardDeleteUrns().isEmpty()) {
          log.info(
              "Detected {} entities to delete (removed or with removed aspects)",
              diff.getHardDeleteUrns().size());
          softDeleteUrnsAsync(context.opContext(), diff.getHardDeleteUrns());

          // Wait for hard-delete completion to prevent race condition with context shutdown
          waitForHardDeleteCompletion();
        } else {
          log.info("No entities to delete");
        }

        // Step 5: Parse and ingest new sample data
        final List<MetadataChangeProposal> allProposals;
        try (InputStream inputStream = new ClassPathResource(filePath).getInputStream()) {
          allProposals =
              McpJsonUtils.parseMcpsFromJsonStream(mapper, inputStream, MAX_PARSE_ERRORS_TO_LOG);
        } catch (Exception e) {
          log.error(
              "Could not parse sample data file at {}. Error: {}", filePath, e.getMessage(), e);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        log.info("Parsed {} MCPs. Marking as sample data...", allProposals.size());

        // Mark all MCPs with sampleData=true in systemMetadata
        for (MetadataChangeProposal mcp : allProposals) {
          markAsSampleData(mcp);
        }

        // Ingest with toggle-aware Status emission
        boolean success =
            ingestSampleDataWithToggleAwareness(context.opContext(), allProposals, toggleOff);
        if (!success) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        // Step 6: Update global settings to reflect sample data availability
        // Only set status if it doesn't already exist - preserve user's toggle state
        log.info("Updating global settings...");
        final AuditStamp auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        GlobalSettingsInfo globalSettingsInfo =
            (GlobalSettingsInfo)
                entityService.getLatestAspect(
                    context.opContext(),
                    Constants.GLOBAL_SETTINGS_URN,
                    Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
        if (globalSettingsInfo == null) {
          globalSettingsInfo = new GlobalSettingsInfo();
        }
        GlobalVisualSettings visualSettings =
            globalSettingsInfo.getVisual() != null
                ? globalSettingsInfo.getVisual()
                : new GlobalVisualSettings();

        SampleDataSettings sampleDataSettings = visualSettings.getSampleDataSettings();
        boolean statusAlreadySet =
            sampleDataSettings != null && sampleDataSettings.getSampleDataStatus() != null;

        if (statusAlreadySet) {
          // Preserve existing user-set status - don't overwrite their preference
          log.info(
              "Preserving existing sample data status: {}",
              sampleDataSettings.getSampleDataStatus());
        } else {
          // No status set yet - use detected toggle state or default to ENABLED
          sampleDataSettings =
              sampleDataSettings != null ? sampleDataSettings : new SampleDataSettings();
          sampleDataSettings.setSampleDataStatus(
              toggleOff ? SampleDataStatus.DISABLED : SampleDataStatus.ENABLED);
          log.info(
              "Setting initial sample data status: {}", sampleDataSettings.getSampleDataStatus());
        }

        visualSettings.setSampleDataSettings(sampleDataSettings);
        globalSettingsInfo.setVisual(visualSettings);
        final MetadataChangeProposal globalSettingsMcp = new MetadataChangeProposal();
        globalSettingsMcp.setEntityUrn(Constants.GLOBAL_SETTINGS_URN);
        globalSettingsMcp.setEntityType(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
        globalSettingsMcp.setAspectName(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
        globalSettingsMcp.setChangeType(ChangeType.UPSERT);
        globalSettingsMcp.setAspect(GenericRecordUtils.serializeAspect(globalSettingsInfo));
        entityService.ingestProposal(context.opContext(), globalSettingsMcp, auditStamp, false);

        // Step 7: Store version pointer
        storeInstalledVersion(context.opContext(), CURRENT_VERSION);

        // Step 8: Mark upgrade as complete
        BootstrapStep.setUpgradeResult(context.opContext(), upgradeIdUrn, entityService);

        log.info(
            "Sample data migration complete: {} -> {}. Ingested {} MCPs.",
            installedVersion != null ? installedVersion : "NONE",
            CURRENT_VERSION,
            allProposals.size());

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Failed to ingest/migrate sample data from {}: {}", filePath, e.getMessage(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } finally {
        // Always shutdown executor to prevent thread leak, regardless of code path taken
        shutdownHardDeleteExecutor();
      }
    };
  }

  @Override
  public boolean isOptional() {
    // Free trial sample data is optional - failures shouldn't block system startup
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (!enabled) {
      log.debug("IngestFreeTrialData is disabled. Skipping.");
      return true;
    }

    if (reprocessEnabled) {
      log.info("IngestFreeTrialData reprocess enabled. Running.");
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            systemOpContext, upgradeIdUrn, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
      return true;
    }

    return false;
  }

  /**
   * Computes a short hash of the file content for versioning. Returns first 8 chars of SHA-256
   * hash.
   *
   * @throws IllegalStateException if the file cannot be read or hash cannot be computed
   */
  private static String computeFileHash(String filePath) {
    try (InputStream inputStream = new ClassPathResource(filePath).getInputStream()) {
      String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (int i = 0; i < 4; i++) { // First 4 bytes = 8 hex chars
        hexString.append(String.format("%02x", hash[i]));
      }
      return hexString.toString();
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Could not compute hash for " + filePath, e);
    }
  }

  /**
   * Marks an MCP with sampleData=true in systemMetadata.properties. This allows easy identification
   * and bulk operations on sample data.
   */
  private void markAsSampleData(MetadataChangeProposal mcp) {
    SystemMetadata systemMetadata = mcp.getSystemMetadata();
    if (systemMetadata == null) {
      systemMetadata = new SystemMetadata();
    }

    StringMap properties = systemMetadata.getProperties();
    if (properties == null) {
      properties = new StringMap();
    }

    properties.put(SAMPLE_DATA_PROPERTY_KEY, SAMPLE_DATA_PROPERTY_VALUE);
    systemMetadata.setProperties(properties);
    mcp.setSystemMetadata(systemMetadata);
  }

  /**
   * Fetch the installed sample data version from platformResource.
   *
   * @return the installed version string, or null if no version is stored (fresh install)
   */
  private String fetchInstalledVersion(OperationContext opContext) {
    try {
      Urn versionUrn = Urn.createFromString(SAMPLE_DATA_VERSION_URN_STRING);
      PlatformResourceInfo info =
          (PlatformResourceInfo)
              entityService.getLatestAspect(
                  opContext, versionUrn, Constants.PLATFORM_RESOURCE_INFO_ASPECT_NAME);

      if (info != null && info.hasValue()) {
        // Deserialize from SerializedValue (blob bytes → JSON string → object)
        String json = info.getValue().getBlob().asString(StandardCharsets.UTF_8);
        SampleDataVersionInfo versionInfo =
            new ObjectMapper().readValue(json, SampleDataVersionInfo.class);
        String installedVersion = versionInfo.getInstalledVersion();
        log.info("Found installed sample data version: {}", installedVersion);
        return installedVersion;
      }
    } catch (Exception e) {
      log.warn("Failed to fetch installed sample data version: {}", e.getMessage());
    }

    log.info("No sample data version found in platformResource (fresh install)");
    return null;
  }

  /**
   * Calculate the migration diff between two manifest versions.
   *
   * <p>Special handling for 001 → 002 (or 001 → current hash): This represents the prefix migration
   * where all URNs changed from unprefixed to sample_data_ prefixed. We need to delete ALL old
   * unprefixed entities.
   *
   * @param opContext operation context
   * @param previousVersion the previous version (null for fresh install)
   * @param currentVersion the current version
   * @return the calculated diff
   */
  private ManifestDiff calculateMigrationDiff(
      OperationContext opContext, String previousVersion, String currentVersion) {
    ManifestDiffCalculator calculator = new ManifestDiffCalculator();

    // Special case: 001 → current version is the prefix migration
    // Delete all unprefixed entities (from 001) before ingesting prefixed ones
    // NOTE: 001_manifest.json is temporary and can be removed once existing free trial
    // instances are migrated to prefixed sample data.
    // The auto-detection logic below (hasUnprefixedSampleData) provides a fallback
    // that works without the 001 manifest file.
    if ("001".equals(previousVersion)) {
      log.info(
          "Detected prefix migration (001 → {}), will hard-delete all unprefixed entities",
          currentVersion);
      return calculator
          .loadManifestOptional("001")
          .map(
              v001 -> {
                Set<String> unprefixedUrns = v001.getEntityUrns();
                log.info(
                    "Will delete {} unprefixed entities from version 001", unprefixedUrns.size());
                return ManifestDiff.builder()
                    .removedUrns(unprefixedUrns)
                    .urnsWithRemovedAspects(Collections.emptySet())
                    .hardDeleteUrns(unprefixedUrns)
                    .build();
              })
          .orElseGet(
              () -> {
                log.warn(
                    "001 manifest not found but version marker says 001. "
                        + "Proceeding with fresh install (no deletions). "
                        + "Unprefixed entities may remain in database.");
                Manifest currentManifest = calculator.loadManifest(currentVersion);
                return calculator.calculateDiffForFreshInstall(currentManifest);
              });
    }

    // No previous version - could be fresh install OR existing instance with unprefixed data
    // Auto-detect by checking if unprefixed sample data exists (fallback for when 001 removed)
    if (previousVersion == null) {
      log.info("No version marker found. Checking for existing unprefixed sample data...");

      if (hasUnprefixedSampleData(opContext)) {
        log.info(
            "Detected existing unprefixed sample data. Treating as version 001 → {} migration",
            currentVersion);
        return calculator
            .loadManifestOptional("001")
            .map(
                v001 -> {
                  Set<String> unprefixedUrns = v001.getEntityUrns();
                  log.info("Will delete {} unprefixed entities", unprefixedUrns.size());
                  return ManifestDiff.builder()
                      .removedUrns(unprefixedUrns)
                      .urnsWithRemovedAspects(Collections.emptySet())
                      .hardDeleteUrns(unprefixedUrns)
                      .build();
                })
            .orElseGet(
                () -> {
                  log.warn(
                      "Unprefixed sample data detected but 001 manifest not found. "
                          + "Cannot determine which entities to delete. "
                          + "Proceeding with fresh install - unprefixed entities may remain.");
                  Manifest currentManifest = calculator.loadManifest(currentVersion);
                  return calculator.calculateDiffForFreshInstall(currentManifest);
                });
      } else {
        log.info("No unprefixed sample data found. Fresh install - no deletions needed");
        Manifest currentManifest = calculator.loadManifest(currentVersion);
        return calculator.calculateDiffForFreshInstall(currentManifest);
      }
    }

    // Normal diff calculation for incremental updates
    log.info("Calculating diff between versions: {} -> {}", previousVersion, currentVersion);
    Manifest current = calculator.loadManifest(currentVersion);

    // Gracefully handle missing previous manifest (e.g., if intermediate version was cleaned up)
    Optional<Manifest> previousOpt = calculator.loadManifestOptional(previousVersion);
    if (previousOpt.isEmpty()) {
      log.warn(
          "Previous manifest {} not found - treating as full re-ingest (delete all + re-ingest)",
          previousVersion);
      // Return diff that will delete all current entities and re-ingest
      return calculator.calculateDiffForFreshInstall(current);
    }

    return calculator.calculateDiff(previousOpt.get(), current);
  }

  /**
   * Soft-delete URNs (emit Status.removed=true) and schedule async hard-delete.
   *
   * <p>Soft-delete happens immediately for instant UX (entities disappear from UI). Hard-delete
   * runs async to avoid blocking ingestion.
   *
   * @param opContext operation context
   * @param urns URNs to delete
   */
  private void softDeleteUrnsAsync(OperationContext opContext, Set<String> urns) {
    if (urns.isEmpty()) {
      return;
    }

    log.info("Soft-deleting {} entities (emitting Status.removed=true)", urns.size());

    final AuditStamp auditStamp;
    try {
      auditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());
    } catch (Exception e) {
      log.error("Failed to create audit stamp: {}", e.getMessage());
      return;
    }

    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(SAMPLE_DATA_PROPERTY_KEY, SAMPLE_DATA_PROPERTY_VALUE);
    systemMetadata.setProperties(properties);

    // Emit Status.removed=true for immediate UX
    for (String urnStr : urns) {
      try {
        Urn urn = Urn.createFromString(urnStr);
        Status status = new Status().setRemoved(true);

        MetadataChangeProposal statusMcp = new MetadataChangeProposal();
        statusMcp.setEntityUrn(urn);
        statusMcp.setEntityType(urn.getEntityType());
        statusMcp.setAspectName(Constants.STATUS_ASPECT_NAME);
        statusMcp.setChangeType(ChangeType.UPSERT);
        statusMcp.setAspect(GenericRecordUtils.serializeAspect(status));
        statusMcp.setSystemMetadata(systemMetadata);

        entityService.ingestProposal(opContext, statusMcp, auditStamp, false);
      } catch (Exception e) {
        log.error("Failed to soft-delete entity {}: {}", urnStr, e.getMessage());
      }
    }

    log.info("Soft-delete complete. Scheduling async hard-delete...");

    // Schedule async hard-delete using dedicated executor for proper lifecycle management
    // This ensures the task won't be orphaned if the common ForkJoinPool shuts down
    hardDeleteFuture =
        CompletableFuture.runAsync(() -> hardDeleteUrns(opContext, urns), hardDeleteExecutor)
            .exceptionally(
                ex -> {
                  log.error("Async hard-delete failed: {}", ex.getMessage(), ex);
                  return null;
                });
  }

  /**
   * Hard-delete URNs (remove all aspects from DB).
   *
   * <p>This runs asynchronously and may take time. Failures are logged but don't block ingestion.
   *
   * <p>SAFETY: Before deleting, we verify each entity still has sampleData=true systemMetadata. If
   * a user ingested real data with the same URN, it would NOT have this marker, so we skip it.
   *
   * @param opContext operation context
   * @param urns URNs to hard-delete
   */
  private void hardDeleteUrns(OperationContext opContext, Set<String> urns) {
    log.info("Starting async hard-delete of {} entities", urns.size());

    int successCount = 0;
    int failureCount = 0;
    int skippedCount = 0;

    for (String urnStr : urns) {
      try {
        Urn urn = Urn.createFromString(urnStr);

        // Safety check: Only delete if entity still has sampleData=true marker
        // This prevents accidentally deleting user data that was ingested with the same URN
        if (!isSampleDataEntity(opContext, urn)) {
          skippedCount++;
          log.debug("Skipping hard-delete of {} - no sampleData marker (may be user data)", urnStr);
          continue;
        }

        entityService.deleteUrn(opContext, urn);
        successCount++;

        if (successCount % 100 == 0) {
          log.info("Hard-deleted {} / {} entities", successCount, urns.size());
        }
      } catch (RejectedExecutionException e) {
        // Shutdown detected - exit gracefully
        log.warn(
            "Thread pool shutdown detected during hard-delete. "
                + "Processed {} / {} entities. Remaining entities are soft-deleted (hidden from UI).",
            successCount,
            urns.size());
        break;
      } catch (Exception e) {
        failureCount++;
        log.error("Failed to hard-delete entity {}: {}", urnStr, e.getMessage());
      }
    }

    log.info(
        "Async hard-delete complete: {} succeeded, {} failed, {} skipped (user data)",
        successCount,
        failureCount,
        skippedCount);
  }

  /**
   * Check if an entity is sample data by checking for sampleData=true in systemMetadata.
   *
   * <p>This is used as a safety check before hard-deleting to avoid accidentally deleting user data
   * that was ingested with the same URN as sample data.
   *
   * @param opContext operation context
   * @param urn the entity URN to check
   * @return true if entity has sampleData=true marker, false otherwise
   */
  private boolean isSampleDataEntity(OperationContext opContext, Urn urn) {
    try {
      // Get entity info including systemMetadata
      // We check the status aspect since it's lightweight and all sample data entities have it
      EnvelopedAspect envelopedAspect =
          entityService.getLatestEnvelopedAspect(
              opContext, urn.getEntityType(), urn, Constants.STATUS_ASPECT_NAME);

      if (envelopedAspect == null) {
        // Entity doesn't exist or has no status aspect - safe to skip (might already be deleted)
        log.debug("Entity {} has no status aspect - skipping hard-delete", urn);
        return false;
      }

      SystemMetadata systemMetadata = envelopedAspect.getSystemMetadata();
      if (systemMetadata == null || !systemMetadata.hasProperties()) {
        // No systemMetadata or properties - not sample data
        log.debug("Entity {} has no systemMetadata properties - not sample data", urn);
        return false;
      }

      String sampleDataValue = systemMetadata.getProperties().get(SAMPLE_DATA_PROPERTY_KEY);
      boolean isSampleData = SAMPLE_DATA_PROPERTY_VALUE.equals(sampleDataValue);

      if (!isSampleData) {
        log.debug("Entity {} sampleData property = {} - not sample data", urn, sampleDataValue);
      }

      return isSampleData;
    } catch (Exception e) {
      // On error, err on the side of caution - don't delete
      log.warn(
          "Error checking if {} is sample data: {}. Skipping hard-delete for safety.",
          urn,
          e.getMessage());
      return false;
    }
  }

  /**
   * Wait for async hard-delete to complete before returning from upgrade step.
   *
   * <p>This ensures the background deletion task completes before the Spring context closes and
   * terminates thread pools.
   *
   * <p>Timeout handling is graceful - if deletion takes too long, we log a warning but don't fail
   * the upgrade since soft-delete already hides entities from UI.
   */
  private void waitForHardDeleteCompletion() {
    if (hardDeleteFuture == null) {
      return;
    }

    try {
      log.info(
          "Waiting for async hard-delete to complete (timeout: {} minutes)...",
          HARD_DELETE_TIMEOUT_MINUTES);
      hardDeleteFuture.get(HARD_DELETE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      log.info("Async hard-delete completed successfully");
    } catch (TimeoutException e) {
      log.warn(
          "Hard-delete timed out after {} minutes. Entities are soft-deleted (hidden from UI) "
              + "but may not be fully removed from database. Cancelling background task.",
          HARD_DELETE_TIMEOUT_MINUTES);
      // Cancel the orphaned task to prevent it from racing with new ingestion
      hardDeleteFuture.cancel(true);
    } catch (ExecutionException e) {
      log.error(
          "Hard-delete failed during execution: {}. Entities are soft-deleted (hidden from UI).",
          e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Hard-delete interrupted. Entities are soft-deleted (hidden from UI).");
    } finally {
      hardDeleteFuture = null;
      // Note: Executor shutdown is handled by the outer finally block in executable()
    }
  }

  /**
   * Gracefully shutdown the hard-delete executor.
   *
   * <p>Attempts orderly shutdown first, then forces shutdown if tasks don't complete quickly.
   */
  private void shutdownHardDeleteExecutor() {
    if (hardDeleteExecutor == null || hardDeleteExecutor.isShutdown()) {
      return;
    }

    log.info("Shutting down hard-delete executor...");
    hardDeleteExecutor.shutdown();
    try {
      if (!hardDeleteExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("Hard-delete executor did not terminate in 30s, forcing shutdown");
        hardDeleteExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      hardDeleteExecutor.shutdownNow();
    }
  }

  /**
   * Check if unprefixed sample data exists by probing for known unprefixed entities.
   *
   * <p>We check multiple entities from version 001 (unprefixed). If ANY exists, we know this is an
   * upgrade from unprefixed → prefixed. Checking multiple URNs increases reliability in case some
   * entities were manually deleted.
   *
   * @param opContext operation context
   * @return true if any unprefixed sample data exists, false otherwise
   */
  private boolean hasUnprefixedSampleData(OperationContext opContext) {
    for (String probeUrnStr : UNPREFIXED_PROBE_URNS) {
      try {
        Urn probeUrn = Urn.createFromString(probeUrnStr);

        // Check if this unprefixed entity exists
        // Use entity type-specific aspect to verify existence
        String aspectToCheck = getAspectForEntityType(probeUrn.getEntityType());
        Object aspect = entityService.getLatestAspect(opContext, probeUrn, aspectToCheck);

        if (aspect != null) {
          log.info("Found unprefixed sample data entity: {}", probeUrnStr);
          return true;
        }
      } catch (Exception e) {
        log.debug(
            "Error checking for unprefixed sample data ({}): {}", probeUrnStr, e.getMessage());
      }
    }

    log.info(
        "No unprefixed sample data entities found (checked {} URNs)", UNPREFIXED_PROBE_URNS.length);
    return false;
  }

  /** Get an appropriate aspect name to check for entity existence based on entity type. */
  private String getAspectForEntityType(String entityType) {
    switch (entityType) {
      case "dataset":
        return Constants.DATASET_PROPERTIES_ASPECT_NAME;
      case "dashboard":
        return Constants.DASHBOARD_INFO_ASPECT_NAME;
      case "chart":
        return Constants.CHART_INFO_ASPECT_NAME;
      default:
        return Constants.STATUS_ASPECT_NAME;
    }
  }

  /**
   * Check if sample data toggle is OFF by querying global settings.
   *
   * @return true if toggle is OFF, false if ON or status unknown
   */
  private boolean isToggleOff(OperationContext opContext) {
    try {
      GlobalSettingsInfo globalSettingsInfo =
          (GlobalSettingsInfo)
              entityService.getLatestAspect(
                  opContext,
                  Constants.GLOBAL_SETTINGS_URN,
                  Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);

      if (globalSettingsInfo == null) {
        log.info("GlobalSettingsInfo is null - no settings exist yet. Defaulting to toggle ON.");
        return false;
      }

      if (!globalSettingsInfo.hasVisual()) {
        log.info("GlobalSettingsInfo has no visual settings. Defaulting to toggle ON.");
        return false;
      }

      if (!globalSettingsInfo.getVisual().hasSampleDataSettings()) {
        log.info("GlobalSettingsInfo has no sampleDataSettings. Defaulting to toggle ON.");
        return false;
      }

      SampleDataStatus status =
          globalSettingsInfo.getVisual().getSampleDataSettings().getSampleDataStatus();
      if (status == null) {
        log.info("SampleDataStatus is null. Defaulting to toggle ON.");
        return false;
      }

      boolean isOff = status == SampleDataStatus.DISABLED;
      log.info("Sample data toggle status: {} (raw value: {})", isOff ? "OFF" : "ON", status);
      return isOff;
    } catch (Exception e) {
      log.warn(
          "Failed to check sample data toggle status: {}. Defaulting to toggle ON.",
          e.getMessage(),
          e);
    }

    // Default to ON if status unknown
    return false;
  }

  /**
   * Ingest sample data MCPs, emitting Status.removed=true FIRST if toggle is OFF.
   *
   * <p>This ensures zero UX flash - entities are never visible when toggle is OFF.
   *
   * @param opContext operation context
   * @param allProposals all MCPs to ingest
   * @param toggleOff true if sample data toggle is OFF
   * @return true if successful, false otherwise
   */
  private boolean ingestSampleDataWithToggleAwareness(
      OperationContext opContext, List<MetadataChangeProposal> allProposals, boolean toggleOff) {

    final AuditStamp auditStamp;
    try {
      auditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());
    } catch (Exception e) {
      log.error("Failed to create audit stamp: {}", e.getMessage());
      return false;
    }

    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(SAMPLE_DATA_PROPERTY_KEY, SAMPLE_DATA_PROPERTY_VALUE);
    systemMetadata.setProperties(properties);

    if (toggleOff) {
      // Emit Status.removed=true FIRST for all entities
      log.info(
          "Toggle is OFF - emitting Status.removed=true for all {} entities FIRST",
          allProposals.size());

      Set<String> allUrns =
          allProposals.stream()
              .map(mcp -> mcp.getEntityUrn().toString())
              .collect(Collectors.toSet());

      for (String urnStr : allUrns) {
        try {
          Urn urn = Urn.createFromString(urnStr);
          Status status = new Status().setRemoved(true);

          MetadataChangeProposal statusMcp = new MetadataChangeProposal();
          statusMcp.setEntityUrn(urn);
          statusMcp.setEntityType(urn.getEntityType());
          statusMcp.setAspectName(Constants.STATUS_ASPECT_NAME);
          statusMcp.setChangeType(ChangeType.UPSERT);
          statusMcp.setAspect(GenericRecordUtils.serializeAspect(status));
          statusMcp.setSystemMetadata(systemMetadata);

          entityService.ingestProposal(opContext, statusMcp, auditStamp, false);
        } catch (Exception e) {
          log.error("Failed to emit Status.removed=true for {}: {}", urnStr, e.getMessage());
        }
      }

      log.info("Status.removed=true emission complete. Now ingesting other aspects...");
    }

    // When toggle is OFF, filter out status aspects from the batch since we already emitted
    // removed=true
    List<MetadataChangeProposal> proposalsToIngest = allProposals;
    if (toggleOff) {
      proposalsToIngest =
          allProposals.stream()
              .filter(mcp -> !Constants.STATUS_ASPECT_NAME.equals(mcp.getAspectName()))
              .collect(Collectors.toList());
      log.info(
          "Filtered out {} status aspects (toggle OFF), ingesting {} remaining MCPs",
          allProposals.size() - proposalsToIngest.size(),
          proposalsToIngest.size());
    }

    // Now ingest all MCPs (batch ingestion)
    log.info("Starting batch ingestion of {} MCPs...", proposalsToIngest.size());

    final AtomicInteger batchesProcessed = new AtomicInteger(0);
    final AtomicInteger batchesFailed = new AtomicInteger(0);
    final int totalMcps = proposalsToIngest.size();

    Iterators.partition(proposalsToIngest.iterator(), batchSize)
        .forEachRemaining(
            batch -> {
              try {
                final AspectsBatch aspectsBatch =
                    AspectsBatchImpl.builder()
                        .mcps(batch, auditStamp, opContext.getRetrieverContext())
                        .build(opContext);

                entityService.ingestProposal(opContext, aspectsBatch, false);

                batchesProcessed.incrementAndGet();
                if (batchesProcessed.get() % 10 == 0 || batchesProcessed.get() == 1) {
                  log.info(
                      "Ingested batch {}. Progress: {} / {}",
                      batchesProcessed.get(),
                      Math.min(batchesProcessed.get() * batchSize, totalMcps),
                      totalMcps);
                }
              } catch (Exception e) {
                batchesFailed.incrementAndGet();
                log.error(
                    "Failed to ingest batch {}: {}", batchesProcessed.get() + 1, e.getMessage(), e);
              }
            });

    if (batchesFailed.get() > 0) {
      log.error(
          "Failed to ingest sample data: {} of {} batches failed",
          batchesFailed.get(),
          batchesProcessed.get() + batchesFailed.get());
      return false;
    }

    log.info("Batch ingestion complete: {} batches succeeded", batchesProcessed.get());
    return true;
  }

  /**
   * Store the current sample data version to platformResource.
   *
   * <p>This method throws on failure to ensure the migration doesn't silently fail to record
   * version, which would cause repeated re-runs on every restart.
   *
   * @param opContext operation context
   * @param version the version to store
   * @throws RuntimeException if version cannot be stored
   */
  private void storeInstalledVersion(OperationContext opContext, String version) {
    try {
      Urn versionUrn = Urn.createFromString(SAMPLE_DATA_VERSION_URN_STRING);

      SampleDataVersionInfo versionInfo = new SampleDataVersionInfo();
      versionInfo.setInstalledVersion(version);
      versionInfo.setLastIngestedAt(System.currentTimeMillis());

      // Serialize to SerializedValue (object → JSON string → blob bytes)
      String json = systemOpContext.getObjectMapper().writeValueAsString(versionInfo);
      com.linkedin.common.SerializedValue serializedValue =
          new com.linkedin.common.SerializedValue();
      serializedValue.setBlob(
          com.linkedin.data.ByteString.copyString(json, StandardCharsets.UTF_8));
      serializedValue.setContentType(com.linkedin.common.SerializedValueContentType.JSON);
      serializedValue.setSchemaType(com.linkedin.common.SerializedValueSchemaType.JSON);
      serializedValue.setSchemaRef("com.linkedin.metadata.SampleDataVersionInfo");

      PlatformResourceInfo info = new PlatformResourceInfo();
      info.setResourceType("sample-data-version");
      info.setPrimaryKey("sample-data-version");
      info.setValue(serializedValue);

      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      MetadataChangeProposal mcp = new MetadataChangeProposal();
      mcp.setEntityUrn(versionUrn);
      mcp.setEntityType(Constants.PLATFORM_RESOURCE_ENTITY_NAME);
      mcp.setAspectName(Constants.PLATFORM_RESOURCE_INFO_ASPECT_NAME);
      mcp.setChangeType(ChangeType.UPSERT);
      mcp.setAspect(GenericRecordUtils.serializeAspect(info));

      entityService.ingestProposal(opContext, mcp, auditStamp, false);

      log.info("Stored sample data version: {}", version);
    } catch (Exception e) {
      // Propagate failure - without version marker, migration will re-run on every restart
      throw new RuntimeException("Failed to store sample data version: " + e.getMessage(), e);
    }
  }
}
