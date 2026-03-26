package com.linkedin.datahub.upgrade.system.entityconsistency;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.SystemMetadataFilter;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import com.linkedin.metadata.config.EntityConsistencyConfiguration.SystemMetadataFilterConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step that fixes consistency issues for configured entity types.
 *
 * <p>Processes entity types sequentially, running configured checks for each type. Uses {@link
 * ConsistencyService} for the actual check execution.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Configurable entity types (defaults to all types with registered checks)
 *   <li>Configurable check IDs (defaults to all default checks for each entity type)
 *   <li>Optional filter configuration for timestamp-based filtering
 *   <li>Shared context across batches for soft-deleted entity tracking
 *   <li>Batch processing with configurable size and delay
 *   <li>Required aspects derived dynamically from check requirements
 * </ul>
 *
 * <p>Uses the system metadata Elasticsearch index to find entities, then fetches entity data from
 * SQL via EntityService. Optional filtering by timestamps (aspectModifiedTime/aspectCreatedTime)
 * and aspect existence.
 */
@Slf4j
public class FixEntityConsistencyStep implements UpgradeStep {

  private static final String UPGRADE_ID = "entity-consistency-v1";

  // State keys for persisting progress in DataHubUpgradeResult.result map
  private static final String KEY_CURRENT_ENTITY_TYPE = "currentEntityType";
  private static final String KEY_SCROLL_ID = "scrollId";
  private static final String KEY_ENTITIES_SCANNED = "entitiesScanned";
  private static final String KEY_ISSUES_FOUND = "issuesFound";
  private static final String KEY_ISSUES_FIXED = "issuesFixed";
  private static final String KEY_ISSUES_FAILED = "issuesFailed";
  private static final String KEY_RUN_START_TIME = "runStartTime";
  private static final String KEY_LAST_COMPLETED_TIME = "lastCompletedTime"; // Regular mode only

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final ConsistencyService consistencyService;
  private final EntityConsistencyConfiguration config;

  /** Config fingerprint computed at construction for upgrade ID uniqueness. */
  private final String configFingerprint;

  public FixEntityConsistencyStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull ConsistencyService consistencyService,
      @Nonnull EntityConsistencyConfiguration config) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.consistencyService = consistencyService;
    this.config = config;
    this.configFingerprint = computeConfigFingerprint();

    log.info(
        "{} initialized: dryRun={}, batchSize={}, batchDelayMs={}, limit={}, gracePeriodSeconds={}, "
            + "reprocessEnabled={}, entityTypes={}, checkIds={}, filterConfig={}, fingerprint={}, targetedRun={}",
        id(),
        config.isDryRun(),
        config.getBatchSize(),
        config.getDelayMs(),
        config.getLimit(),
        config.getGracePeriodSeconds(),
        config.getReprocess() != null && config.getReprocess().isEnabled(),
        config.getEntityTypes() == null || config.getEntityTypes().isEmpty()
            ? "[ALL]"
            : config.getEntityTypes(),
        config.getCheckIds() == null || config.getCheckIds().isEmpty()
            ? "[ALL DEFAULTS]"
            : config.getCheckIds(),
        config.getSystemMetadataFilterConfig() != null
                && config.getSystemMetadataFilterConfig().hasAnyConfig()
            ? "[CONFIGURED]"
            : "[DEFAULT]",
        configFingerprint,
        isTargetedRun());
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Computes a fingerprint from configuration to ensure different configs are tracked separately.
   * Includes timestamps and limit in fingerprint for targeted runs (so different configurations are
   * tracked separately).
   */
  private String computeConfigFingerprint() {
    StringBuilder sb = new StringBuilder();
    if (config.getCheckIds() != null && !config.getCheckIds().isEmpty()) {
      sb.append(config.getCheckIds().stream().sorted().collect(Collectors.joining(",")));
    }
    if (config.getEntityTypes() != null && !config.getEntityTypes().isEmpty()) {
      sb.append("|")
          .append(config.getEntityTypes().stream().sorted().collect(Collectors.joining(",")));
    }
    // Include limit in fingerprint - different limits are tracked separately
    if (config.getLimit() > 0) {
      sb.append("|limit:").append(config.getLimit());
    }
    SystemMetadataFilterConfig filterConfig = config.getSystemMetadataFilterConfig();
    if (filterConfig != null) {
      if (filterConfig.getAspectFilters() != null && !filterConfig.getAspectFilters().isEmpty()) {
        sb.append("|aspects:")
            .append(
                filterConfig.getAspectFilters().stream().sorted().collect(Collectors.joining(",")));
      }
      // Include timestamps in fingerprint for targeted runs
      if (filterConfig.getGePitEpochMs() != null) {
        sb.append("|ge:").append(filterConfig.getGePitEpochMs());
      }
      if (filterConfig.getLePitEpochMs() != null) {
        sb.append("|le:").append(filterConfig.getLePitEpochMs());
      }
    }
    return Integer.toHexString(sb.toString().hashCode());
  }

  /**
   * Returns true if this is a limited run (limit > 0). Limited runs process only a subset of
   * entities and should NOT support resume or incremental processing.
   */
  private boolean isLimitedRun() {
    return config.getLimit() > 0;
  }

  /**
   * Returns true if this is a targeted run that should not support incremental processing. This
   * includes:
   *
   * <ul>
   *   <li>Explicit timestamp filters (gePitEpochMs/lePitEpochMs) - supports resume but not
   *       incremental
   *   <li>Limit > 0 (processing only a subset) - supports neither resume nor incremental
   * </ul>
   */
  private boolean isTargetedRun() {
    // Limited runs are targeted
    if (isLimitedRun()) {
      return true;
    }
    // Explicit timestamp filters = targeted run (resume allowed, no incremental)
    SystemMetadataFilterConfig filterConfig = config.getSystemMetadataFilterConfig();
    return filterConfig != null
        && (filterConfig.getGePitEpochMs() != null || filterConfig.getLePitEpochMs() != null);
  }

  /** Build upgrade URN with config fingerprint for overall completion tracking. */
  private Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(UPGRADE_ID + "_" + configFingerprint);
  }

  /** Build per-entity-type upgrade URN for entity type progress tracking. */
  private Urn getUpgradeIdUrn(String entityType) {
    return BootstrapStep.getUpgradeUrn(UPGRADE_ID + "_" + configFingerprint + "_" + entityType);
  }

  @Override
  public java.util.function.Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting {} (dryRun={}, fingerprint={})", id(), config.isDryRun(), configFingerprint);
    if (config.isDryRun()) {
      log.info(
          "Running in DRY-RUN mode. No changes will be made. "
              + "Set systemUpdate.entityConsistency.dryRun=false to apply fixes.");
    }
    return (context) -> {
      try {
        // Check for previous run state
        Optional<DataHubUpgradeResult> prevResult =
            context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

        // Determine run start time and incremental filter
        long runStartTime = System.currentTimeMillis();
        Long incrementalFilterTime = null;

        if (prevResult.isPresent() && !isLimitedRun()) {
          // Limited runs don't support resume or incremental - always start fresh
          DataHubUpgradeResult result = prevResult.get();
          Map<String, String> resultMap = result.getResult();

          if (DataHubUpgradeState.IN_PROGRESS.equals(result.getState())) {
            // Resume from IN_PROGRESS state (only for non-limited runs)
            log.info("{}: Resuming from IN_PROGRESS state", getUpgradeIdUrn());
            if (resultMap != null && resultMap.containsKey(KEY_RUN_START_TIME)) {
              runStartTime = Long.parseLong(resultMap.get(KEY_RUN_START_TIME));
            }
          } else if (DataHubUpgradeState.SUCCEEDED.equals(result.getState()) && !isTargetedRun()) {
            // Regular mode: apply incremental filter from last success
            if (resultMap != null && resultMap.containsKey(KEY_LAST_COMPLETED_TIME)) {
              incrementalFilterTime = Long.parseLong(resultMap.get(KEY_LAST_COMPLETED_TIME));
              log.info(
                  "{}: Incremental mode - processing entities modified since {}",
                  getUpgradeIdUrn(),
                  incrementalFilterTime);
            }
          }
        } else if (isLimitedRun()) {
          log.info("{}: Limited run - starting fresh (no resume/incremental)", getUpgradeIdUrn());
        }

        // Build shared context across all entity types
        CheckContext ctx = consistencyService.buildContext(opContext);

        int totalEntitiesScanned = 0;
        int totalIssuesFound = 0;
        int totalFixed = 0;
        int totalFailed = 0;

        // Determine entity types to process
        Set<String> typesToProcess = resolveEntityTypes();

        // Process each entity type
        for (String entityType : typesToProcess) {
          log.info("Processing entity type: {}", entityType);

          int[] typeResults =
              processEntityType(entityType, ctx, context, runStartTime, incrementalFilterTime);
          totalEntitiesScanned += typeResults[0];
          totalIssuesFound += typeResults[1];
          totalFixed += typeResults[2];
          totalFailed += typeResults[3];

          // Clear caches between entity types to prevent memory buildup
          ctx.clearCaches();

          // Check if we've hit the limit
          if (config.getLimit() > 0 && totalEntitiesScanned >= config.getLimit()) {
            log.info("Reached limit of {} total entities", config.getLimit());
            break;
          }
        }

        // Final report
        log.info(
            "{} Completed: entities={} issues={} fixed={} failed={}",
            config.isDryRun() ? "[DRY-RUN]" : "",
            totalEntitiesScanned,
            totalIssuesFound,
            totalFixed,
            totalFailed);

        context
            .report()
            .addLine(
                String.format(
                    "%sProcessed %d entities (%s), found %d issues, fixed %d, failed %d",
                    config.isDryRun() ? "[DRY-RUN] " : "",
                    totalEntitiesScanned,
                    String.join(", ", typesToProcess),
                    totalIssuesFound,
                    totalFixed,
                    totalFailed));

        // Mark as succeeded only if not in dry-run mode
        if (!config.isDryRun()) {
          // Store lastCompletedTime for incremental support (regular mode only)
          Map<String, String> successState = new HashMap<>();
          successState.put(KEY_ENTITIES_SCANNED, String.valueOf(totalEntitiesScanned));
          successState.put(KEY_ISSUES_FOUND, String.valueOf(totalIssuesFound));
          successState.put(KEY_ISSUES_FIXED, String.valueOf(totalFixed));
          successState.put(KEY_ISSUES_FAILED, String.valueOf(totalFailed));
          if (!isTargetedRun()) {
            successState.put(KEY_LAST_COMPLETED_TIME, String.valueOf(System.currentTimeMillis()));
          }

          BootstrapStep.setUpgradeResult(
              opContext,
              getUpgradeIdUrn(),
              entityService,
              DataHubUpgradeState.SUCCEEDED,
              successState);
          context.report().addLine("State updated: " + getUpgradeIdUrn());
        } else {
          log.info(
              "[DRY-RUN] Completed consistency check. No changes were made. "
                  + "Re-run with dryRun=false to apply fixes.");
          context.report().addLine("[DRY-RUN] Completed - no changes made");
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Error during consistency check: {}", e.getMessage(), e);
        context.report().addLine("Error: " + e.getMessage());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Process an entity type with pagination, supporting resume and incremental processing.
   *
   * @param entityType the entity type to process
   * @param ctx shared check context
   * @param upgradeContext upgrade context for state persistence
   * @param runStartTime timestamp when this run started (for progress tracking)
   * @param incrementalFilterTime optional timestamp for incremental filtering (regular mode only)
   * @return array of [scanned, issues, fixed, failed]
   */
  private int[] processEntityType(
      String entityType,
      CheckContext ctx,
      UpgradeContext upgradeContext,
      long runStartTime,
      @Nullable Long incrementalFilterTime) {

    int scanned = 0;
    int issues = 0;
    int fixed = 0;
    int failed = 0;
    String scrollId = null;

    // Check for previous IN_PROGRESS state for this entity type to resume
    // (limited runs don't support resume - always start fresh)
    if (!isLimitedRun()) {
      Optional<DataHubUpgradeResult> prevTypeResult =
          upgradeContext
              .upgrade()
              .getUpgradeResult(opContext, getUpgradeIdUrn(entityType), entityService);

      if (prevTypeResult.isPresent()
          && DataHubUpgradeState.IN_PROGRESS.equals(prevTypeResult.get().getState())) {
        Map<String, String> prevState = prevTypeResult.get().getResult();
        if (prevState != null) {
          scrollId = prevState.get(KEY_SCROLL_ID);
          scanned = parseIntOrDefault(prevState.get(KEY_ENTITIES_SCANNED), 0);
          issues = parseIntOrDefault(prevState.get(KEY_ISSUES_FOUND), 0);
          fixed = parseIntOrDefault(prevState.get(KEY_ISSUES_FIXED), 0);
          failed = parseIntOrDefault(prevState.get(KEY_ISSUES_FAILED), 0);
          log.info(
              "{}: Resuming from scrollId={}, scanned={}, issues={}",
              getUpgradeIdUrn(entityType),
              scrollId != null
                  ? scrollId.substring(0, Math.min(20, scrollId.length())) + "..."
                  : null,
              scanned,
              issues);
        }
      }
    }

    // Build filter config, applying incremental filter if specified
    SystemMetadataFilterConfig effectiveFilterConfig =
        buildEffectiveFilterConfig(incrementalFilterTime);
    // Convert to service-layer filter type
    SystemMetadataFilter serviceFilter = SystemMetadataFilter.from(effectiveFilterConfig);

    do {
      // Fetch and check a batch for this entity type
      CheckResult checkResult =
          consistencyService.checkBatch(
              opContext,
              CheckBatchRequest.builder()
                  .entityType(entityType)
                  .checkIds(config.getCheckIds())
                  .batchSize(config.getBatchSize())
                  .scrollId(scrollId)
                  .filter(serviceFilter)
                  .build(),
              ctx);

      if (checkResult.getEntitiesScanned() == 0) {
        break;
      }

      scanned += checkResult.getEntitiesScanned();
      issues += checkResult.getIssuesFound();

      // Log and fix issues
      List<ConsistencyIssue> issuesList = checkResult.getIssues();
      if (!issuesList.isEmpty()) {
        logIssues(issuesList);
        ConsistencyFixResult fixResult =
            consistencyService.fixIssues(opContext, issuesList, config.isDryRun());
        fixed += fixResult.getEntitiesFixed();
        failed += fixResult.getEntitiesFailed();
      }

      scrollId = checkResult.getScrollId();

      // Save progress after each batch (unless dry-run or limited run)
      // Limited runs don't support resume, so no need to save progress
      if (!config.isDryRun() && !isLimitedRun()) {
        saveProgress(
            upgradeContext, entityType, scrollId, scanned, issues, fixed, failed, runStartTime);
      }

      // Check limit
      if (config.getLimit() > 0 && scanned >= config.getLimit()) {
        log.info("Reached limit of {} entities for {}", config.getLimit(), entityType);
        break;
      }

      // Apply delay between batches
      applyBatchDelay();

    } while (scrollId != null);

    // Mark entity type as completed (clear IN_PROGRESS state)
    if (!config.isDryRun()) {
      BootstrapStep.setUpgradeResult(
          opContext,
          getUpgradeIdUrn(entityType),
          entityService,
          DataHubUpgradeState.SUCCEEDED,
          Map.of(
              KEY_ENTITIES_SCANNED, String.valueOf(scanned),
              KEY_ISSUES_FOUND, String.valueOf(issues),
              KEY_ISSUES_FIXED, String.valueOf(fixed),
              KEY_ISSUES_FAILED, String.valueOf(failed)));
    }

    log.info("Completed {}: {} entities scanned, {} issues found", entityType, scanned, issues);

    return new int[] {scanned, issues, fixed, failed};
  }

  /**
   * Build effective filter config, applying incremental filter time and grace period.
   *
   * <p>The grace period excludes entities modified within the specified window to avoid false
   * positives from eventual consistency race conditions.
   */
  private SystemMetadataFilterConfig buildEffectiveFilterConfig(
      @Nullable Long incrementalFilterTime) {
    SystemMetadataFilterConfig baseConfig = config.getSystemMetadataFilterConfig();
    long gracePeriodSeconds = config.getGracePeriodSeconds();

    // If no modifications needed, return base config
    if (incrementalFilterTime == null && gracePeriodSeconds <= 0) {
      return baseConfig;
    }

    // Create new config with filters applied
    SystemMetadataFilterConfig effectiveConfig = new SystemMetadataFilterConfig();
    if (baseConfig != null) {
      effectiveConfig.setAspectFilters(baseConfig.getAspectFilters());
      effectiveConfig.setIncludeSoftDeleted(baseConfig.getIncludeSoftDeleted());

      // Handle gePitEpochMs (incremental filter)
      Long configuredGe = baseConfig.getGePitEpochMs();
      if (incrementalFilterTime != null) {
        if (configuredGe != null) {
          effectiveConfig.setGePitEpochMs(Math.max(configuredGe, incrementalFilterTime));
        } else {
          effectiveConfig.setGePitEpochMs(incrementalFilterTime);
        }
      } else {
        effectiveConfig.setGePitEpochMs(configuredGe);
      }

      // Handle lePitEpochMs (grace period) - only apply if not explicitly configured
      Long configuredLe = baseConfig.getLePitEpochMs();
      if (configuredLe != null) {
        // User explicitly configured lePitEpochMs, honor their setting
        effectiveConfig.setLePitEpochMs(configuredLe);
      } else if (gracePeriodSeconds > 0) {
        // Apply grace period
        effectiveConfig.setLePitEpochMs(System.currentTimeMillis() - (gracePeriodSeconds * 1000L));
      }
    } else {
      // No base config, apply filters directly
      if (incrementalFilterTime != null) {
        effectiveConfig.setGePitEpochMs(incrementalFilterTime);
      }
      if (gracePeriodSeconds > 0) {
        effectiveConfig.setLePitEpochMs(System.currentTimeMillis() - (gracePeriodSeconds * 1000L));
      }
    }

    if (effectiveConfig.getGePitEpochMs() != null || effectiveConfig.getLePitEpochMs() != null) {
      log.info(
          "Applying time filters: gePitEpochMs={}, lePitEpochMs={} (gracePeriodSeconds={})",
          effectiveConfig.getGePitEpochMs(),
          effectiveConfig.getLePitEpochMs(),
          gracePeriodSeconds);
    }
    return effectiveConfig;
  }

  /** Save progress state for resume capability. */
  private void saveProgress(
      UpgradeContext upgradeContext,
      String entityType,
      @Nullable String scrollId,
      int scanned,
      int issues,
      int fixed,
      int failed,
      long runStartTime) {

    Map<String, String> state = new HashMap<>();
    state.put(KEY_CURRENT_ENTITY_TYPE, entityType);
    if (scrollId != null) {
      state.put(KEY_SCROLL_ID, scrollId);
    }
    state.put(KEY_ENTITIES_SCANNED, String.valueOf(scanned));
    state.put(KEY_ISSUES_FOUND, String.valueOf(issues));
    state.put(KEY_ISSUES_FIXED, String.valueOf(fixed));
    state.put(KEY_ISSUES_FAILED, String.valueOf(failed));
    state.put(KEY_RUN_START_TIME, String.valueOf(runStartTime));

    // Save per-entity-type progress
    upgradeContext
        .upgrade()
        .setUpgradeResult(
            opContext,
            getUpgradeIdUrn(entityType),
            entityService,
            DataHubUpgradeState.IN_PROGRESS,
            state);

    // Also update overall progress
    upgradeContext
        .upgrade()
        .setUpgradeResult(
            opContext, getUpgradeIdUrn(), entityService, DataHubUpgradeState.IN_PROGRESS, state);
  }

  /** Parse int with default fallback. */
  private int parseIntOrDefault(@Nullable String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /** Resolve entity types to process based on configuration. */
  private Set<String> resolveEntityTypes() {
    List<String> entityTypes = config.getEntityTypes();
    if (entityTypes != null && !entityTypes.isEmpty()) {
      return Set.copyOf(entityTypes);
    }
    // Default to all entity types that have registered checks
    return consistencyService.getCheckRegistry().getDefaultEntityTypes();
  }

  /** Log issues found. */
  private void logIssues(List<ConsistencyIssue> issues) {
    for (ConsistencyIssue issue : issues) {
      log.info(
          "[{}] {} entity={} check={} description={}",
          config.isDryRun() ? "DRY-RUN" : "APPLY",
          issue.getFixType(),
          issue.getEntityUrn(),
          issue.getCheckId(),
          issue.getDescription());
    }
  }

  /** Apply delay between batches if configured. */
  private void applyBatchDelay() {
    if (config.getDelayMs() > 0) {
      try {
        Thread.sleep(config.getDelayMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Batch delay interrupted");
      }
    }
  }

  @Override
  public boolean skip(UpgradeContext context) {
    // Never skip if reprocess is explicitly enabled
    if (config.getReprocess() != null && config.getReprocess().isEnabled()) {
      log.info("{}: Reprocess enabled, not skipping.", getUpgradeIdUrn());
      return false;
    }

    // Check for previous run state with this config fingerprint
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    if (prevResult.isEmpty()) {
      log.info("{}: No previous run found, proceeding.", getUpgradeIdUrn());
      return false;
    }

    DataHubUpgradeResult result = prevResult.get();
    DataHubUpgradeState state = result.getState();

    // For IN_PROGRESS state, allow resume UNLESS it's a limited run
    if (DataHubUpgradeState.IN_PROGRESS.equals(state)) {
      if (isLimitedRun()) {
        // Limited runs don't support resume - start fresh
        log.info(
            "{}: Previous run IN_PROGRESS but limited run, starting fresh.", getUpgradeIdUrn());
        return false; // Don't skip - but will start fresh (no scrollId restore for limited runs)
      }
      log.info("{}: Previous run IN_PROGRESS, resuming.", getUpgradeIdUrn());
      return false;
    }

    // For SUCCEEDED state, behavior depends on run mode
    if (DataHubUpgradeState.SUCCEEDED.equals(state)) {
      if (isTargetedRun()) {
        // Targeted runs: skip if already succeeded (no incremental support)
        log.info("{}: Targeted run already succeeded, skipping.", getUpgradeIdUrn());
        return true;
      } else {
        // Regular runs: don't skip - allow incremental processing
        log.info(
            "{}: Regular run succeeded, proceeding with incremental processing.",
            getUpgradeIdUrn());
        return false;
      }
    }

    // For ABORTED or other states, skip
    log.info("{}: Previous state={}, skipping.", getUpgradeIdUrn(), state);
    return true;
  }

  /** Returns whether the upgrade should proceed if the step fails. */
  @Override
  public boolean isOptional() {
    return true;
  }
}
