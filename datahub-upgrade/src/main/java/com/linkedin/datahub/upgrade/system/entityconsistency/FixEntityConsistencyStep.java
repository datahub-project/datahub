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
import com.linkedin.metadata.config.ConsistencyCheckMode;
import com.linkedin.metadata.config.ConsistencyCheckSchedule;
import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import com.linkedin.metadata.config.EntityConsistencyConfiguration.SystemMetadataFilterConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
 * <p>Per-check {@link ConsistencyCheckMode} controls whether each check is disabled, dry-run, or
 * active. Job-level {@code dryRun} is a safety ceiling. Cadence schedules (e.g. monthly for {@code
 * orphan-index-document}) apply only to this upgrade path.
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
  private static final String KEY_LAST_COMPLETED_TIME =
      ConsistencyCheckCadence.KEY_LAST_COMPLETED_TIME;

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final ConsistencyService consistencyService;
  private final EntityConsistencyConfiguration config;
  private final Clock clock;

  /** Config fingerprint computed at construction for upgrade ID uniqueness. */
  private final String configFingerprint;

  public FixEntityConsistencyStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull ConsistencyService consistencyService,
      @Nonnull EntityConsistencyConfiguration config) {
    this(opContext, entityService, consistencyService, config, Clock.systemUTC());
  }

  public FixEntityConsistencyStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull ConsistencyService consistencyService,
      @Nonnull EntityConsistencyConfiguration config,
      @Nonnull Clock clock) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.consistencyService = consistencyService;
    this.config = config;
    this.clock = clock;
    this.configFingerprint = computeConfigFingerprint();

    log.info(
        "{} initialized: dryRun={}, batchSize={}, batchDelayMs={}, limit={}, gracePeriodSeconds={}, "
            + "reprocessEnabled={}, entityTypes={}, checkIds={}, filterConfig={}, fingerprint={}, targetedRun={}, "
            + "orphanMode={}, orphanSchedule={}",
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
        isTargetedRun(),
        config.getCheckMode(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID),
        config.getCheckSchedule(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID));
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

  private boolean isReprocessEnabled() {
    return config.getReprocess() != null && config.getReprocess().isEnabled();
  }

  /** Build upgrade URN with config fingerprint for overall completion tracking. */
  private Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(UPGRADE_ID + "_" + configFingerprint);
  }

  /** Build per-entity-type upgrade URN for entity type progress tracking. */
  private Urn getUpgradeIdUrn(String entityType) {
    return BootstrapStep.getUpgradeUrn(UPGRADE_ID + "_" + configFingerprint + "_" + entityType);
  }

  private Urn getOrphanCadenceUrn() {
    return BootstrapStep.getUpgradeUrn(
        ConsistencyCheckCadence.checkUpgradeId(
            EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID));
  }

  /**
   * Whether {@code orphan-index-document} should run on this upgrade pass (mode not disabled and
   * cadence due, or explicitly listed in checkIds).
   *
   * <p>Requires the check to be present in {@code checks} config (or listed in {@code checkIds});
   * otherwise it is not auto-scheduled on upgrade.
   */
  public boolean isOrphanCheckDue(@Nonnull UpgradeContext context) {
    boolean explicitlyRequested =
        config.getCheckIds() != null
            && config
                .getCheckIds()
                .contains(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
    boolean configured =
        config
            .getChecksOrEmpty()
            .containsKey(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);

    if (!explicitlyRequested && !configured) {
      return false;
    }

    ConsistencyCheckMode mode =
        config.getCheckMode(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
    if (mode.isDisabled()) {
      return false;
    }

    if (explicitlyRequested) {
      return true;
    }

    ConsistencyCheckSchedule schedule =
        config.getCheckSchedule(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
    Long lastCompleted = null;
    Optional<DataHubUpgradeResult> prev =
        context.upgrade().getUpgradeResult(opContext, getOrphanCadenceUrn(), entityService);
    if (prev.isPresent() && prev.get().getResult() != null) {
      lastCompleted = ConsistencyCheckCadence.parseLastCompleted(prev.get().getResult());
    }
    return ConsistencyCheckCadence.isDue(schedule, lastCompleted, isReprocessEnabled(), clock);
  }

  @Override
  public java.util.function.Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting {} (dryRun={}, fingerprint={})", id(), config.isDryRun(), configFingerprint);
    if (config.isDryRun()) {
      log.info(
          "Job-level dryRun=true: safety ceiling — no check will apply fixes. "
              + "Set systemUpdate.entityConsistency.dryRun=false and per-check mode=active to apply.");
    }
    return (context) -> {
      try {
        boolean orphanDue = isOrphanCheckDue(context);
        log.info(
            "orphan-index-document due={} mode={} schedule={}",
            orphanDue,
            config.getCheckMode(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID),
            config.getCheckSchedule(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID));

        // Check for previous run state
        Optional<DataHubUpgradeResult> prevResult =
            context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

        // Determine run start time and incremental filter
        long runStartTime = clock.millis();
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
          } else if (DataHubUpgradeState.SUCCEEDED.equals(result.getState())
              && !isTargetedRun()
              && !orphanDue) {
            // Regular mode: apply incremental filter from last success.
            // Orphan-due runs must full-scan (old orphan docs would be filtered out otherwise).
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

        if (orphanDue) {
          incrementalFilterTime = null;
          log.info(
              "Orphan check due — full system-metadata scan (grace period only, no incremental gePit)");
        }

        // Build shared context across all entity types
        CheckContext ctx = consistencyService.buildContext(opContext);

        int totalEntitiesScanned = 0;
        int totalIssuesFound = 0;
        int totalFixed = 0;
        int totalFailed = 0;

        List<String> effectiveCheckIds = resolveEffectiveCheckIds(orphanDue);
        Set<String> typesToProcess = resolveEntityTypes(orphanDue);

        log.info(
            "Processing entity types={} checkIds={}",
            typesToProcess,
            effectiveCheckIds.isEmpty() ? "[DEFAULTS]" : effectiveCheckIds);

        // Process each entity type
        for (String entityType : typesToProcess) {
          log.info("Processing entity type: {}", entityType);

          int[] typeResults =
              processEntityType(
                  entityType, ctx, context, runStartTime, incrementalFilterTime, effectiveCheckIds);
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
        boolean anyApply = !config.isDryRun() && totalFixed > 0;
        log.info(
            "{} Completed: entities={} issues={} fixed={} failed={} orphanDue={}",
            config.isDryRun() ? "[JOB-DRY-RUN]" : (anyApply ? "" : "[PER-CHECK-DRY-RUN]"),
            totalEntitiesScanned,
            totalIssuesFound,
            totalFixed,
            totalFailed,
            orphanDue);

        context
            .report()
            .addLine(
                String.format(
                    "Processed %d entities (%s), found %d issues, fixed %d, failed %d (orphanDue=%s)",
                    totalEntitiesScanned,
                    String.join(", ", typesToProcess),
                    totalIssuesFound,
                    totalFixed,
                    totalFailed,
                    orphanDue));

        // Persist overall success for incremental support (job dryRun no longer blocks this —
        // per-check modes control writes; cadence/resume state still needed).
        Map<String, String> successState = new HashMap<>();
        successState.put(KEY_ENTITIES_SCANNED, String.valueOf(totalEntitiesScanned));
        successState.put(KEY_ISSUES_FOUND, String.valueOf(totalIssuesFound));
        successState.put(KEY_ISSUES_FIXED, String.valueOf(totalFixed));
        successState.put(KEY_ISSUES_FAILED, String.valueOf(totalFailed));
        if (!isTargetedRun() && !orphanDue) {
          // When orphan just full-scanned, don't set incremental cursor to "now" in a way that
          // hides unfinished work; still record completion time for regular incremental runs.
          successState.put(KEY_LAST_COMPLETED_TIME, String.valueOf(clock.millis()));
        } else if (!isTargetedRun()) {
          // After orphan full scan, still record completion so next non-orphan run can be
          // incremental.
          successState.put(KEY_LAST_COMPLETED_TIME, String.valueOf(clock.millis()));
        }

        BootstrapStep.setUpgradeResult(
            opContext,
            getUpgradeIdUrn(),
            entityService,
            DataHubUpgradeState.SUCCEEDED,
            successState);
        context.report().addLine("State updated: " + getUpgradeIdUrn());

        if (orphanDue) {
          persistOrphanCadenceCompletion();
          context.report().addLine("Orphan check cadence updated: " + getOrphanCadenceUrn());
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Error during consistency check: {}", e.getMessage(), e);
        context.report().addLine("Error: " + e.getMessage());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void persistOrphanCadenceCompletion() {
    Map<String, String> cadenceState = new HashMap<>();
    cadenceState.put(KEY_LAST_COMPLETED_TIME, String.valueOf(clock.millis()));
    BootstrapStep.setUpgradeResult(
        opContext,
        getOrphanCadenceUrn(),
        entityService,
        DataHubUpgradeState.SUCCEEDED,
        cadenceState);
  }

  /**
   * Resolve check IDs for this run.
   *
   * <p>When orphan is due, includes {@code orphan-index-document}. Disabled checks are omitted.
   * Explicit {@code checkIds} are filtered by mode; empty means defaults (+ orphan when due).
   */
  @Nonnull
  public List<String> resolveEffectiveCheckIds(boolean orphanDue) {
    LinkedHashSet<String> ids = new LinkedHashSet<>();
    if (config.getCheckIds() != null && !config.getCheckIds().isEmpty()) {
      for (String checkId : config.getCheckIds()) {
        if (!config.getCheckMode(checkId).isDisabled()) {
          ids.add(checkId);
        }
      }
    } else {
      // Default checks: non-disabled registered default check ids + orphan when due
      consistencyService.getCheckRegistry().getDefaultChecks().stream()
          .map(c -> c.getId())
          .filter(id -> !config.getCheckMode(id).isDisabled())
          .forEach(ids::add);
    }

    if (orphanDue) {
      ConsistencyCheckMode orphanMode =
          config.getCheckMode(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
      if (!orphanMode.isDisabled()) {
        ids.add(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
      }
    } else {
      // Exclude orphan unless explicitly requested (handled via orphanDue when listed)
      ids.remove(EntityConsistencyConfiguration.ORPHAN_INDEX_DOCUMENT_CHECK_ID);
    }
    return new ArrayList<>(ids);
  }

  /**
   * Process an entity type with pagination, supporting resume and incremental processing.
   *
   * @return array of [scanned, issues, fixed, failed]
   */
  private int[] processEntityType(
      String entityType,
      CheckContext ctx,
      UpgradeContext upgradeContext,
      long runStartTime,
      @Nullable Long incrementalFilterTime,
      @Nonnull List<String> effectiveCheckIds) {

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
                  .checkIds(effectiveCheckIds)
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

      // Log and fix issues with per-check mode
      List<ConsistencyIssue> issuesList = checkResult.getIssues();
      if (!issuesList.isEmpty()) {
        logIssues(issuesList);
        int[] fixCounts = fixIssuesByMode(issuesList);
        fixed += fixCounts[0];
        failed += fixCounts[1];
      }

      scrollId = checkResult.getScrollId();

      // Save progress after each batch (limited runs don't support resume)
      if (!isLimitedRun()) {
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

    log.info("Completed {}: {} entities scanned, {} issues found", entityType, scanned, issues);

    return new int[] {scanned, issues, fixed, failed};
  }

  /**
   * Apply fixes partitioned by per-check mode. Job-level dryRun is a ceiling.
   *
   * @return [fixed, failed]
   */
  private int[] fixIssuesByMode(@Nonnull List<ConsistencyIssue> issuesList) {
    List<ConsistencyIssue> applyIssues = new ArrayList<>();
    List<ConsistencyIssue> dryRunIssues = new ArrayList<>();

    for (ConsistencyIssue issue : issuesList) {
      if (config.shouldApplyFixes(issue.getCheckId())) {
        applyIssues.add(issue);
      } else {
        dryRunIssues.add(issue);
      }
    }

    int fixed = 0;
    int failed = 0;

    if (!dryRunIssues.isEmpty()) {
      ConsistencyFixResult dryResult = consistencyService.fixIssues(opContext, dryRunIssues, true);
      fixed += dryResult.getEntitiesFixed();
      failed += dryResult.getEntitiesFailed();
    }
    if (!applyIssues.isEmpty()) {
      ConsistencyFixResult applyResult =
          consistencyService.fixIssues(opContext, applyIssues, false);
      fixed += applyResult.getEntitiesFixed();
      failed += applyResult.getEntitiesFailed();
    }
    return new int[] {fixed, failed};
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

    // If no modifications needed, return base config (still prefers keyAspectOnly when configured)
    if (incrementalFilterTime == null && gracePeriodSeconds <= 0) {
      return ensureKeyAspectOnlyDefault(baseConfig);
    }

    // Create new config with filters applied
    SystemMetadataFilterConfig effectiveConfig = new SystemMetadataFilterConfig();
    if (baseConfig != null) {
      effectiveConfig.setAspectFilters(baseConfig.getAspectFilters());
      effectiveConfig.setIncludeSoftDeleted(baseConfig.getIncludeSoftDeleted());
      effectiveConfig.setKeyAspectOnly(baseConfig.getKeyAspectOnly());
      effectiveConfig.setEntityTypes(baseConfig.getEntityTypes());

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
        effectiveConfig.setLePitEpochMs(clock.millis() - (gracePeriodSeconds * 1000L));
      }
    } else {
      // No base config, apply filters directly
      if (incrementalFilterTime != null) {
        effectiveConfig.setGePitEpochMs(incrementalFilterTime);
      }
      if (gracePeriodSeconds > 0) {
        effectiveConfig.setLePitEpochMs(clock.millis() - (gracePeriodSeconds * 1000L));
      }
    }

    if (effectiveConfig.getGePitEpochMs() != null || effectiveConfig.getLePitEpochMs() != null) {
      log.info(
          "Applying time filters: gePitEpochMs={}, lePitEpochMs={} (gracePeriodSeconds={})",
          effectiveConfig.getGePitEpochMs(),
          effectiveConfig.getLePitEpochMs(),
          gracePeriodSeconds);
    }
    return ensureKeyAspectOnlyDefault(effectiveConfig);
  }

  /**
   * Job default: key-aspect-only discovery unless explicitly set to false. Keeps scan volume down
   * for orphan and large entity-type passes.
   */
  @Nonnull
  private SystemMetadataFilterConfig ensureKeyAspectOnlyDefault(
      @Nullable SystemMetadataFilterConfig filterConfig) {
    SystemMetadataFilterConfig effective =
        filterConfig != null ? filterConfig : new SystemMetadataFilterConfig();
    if (effective.getKeyAspectOnly() == null) {
      effective.setKeyAspectOnly(true);
    }
    return effective;
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

  /**
   * Resolve entity types to process.
   *
   * <p>Priority:
   *
   * <ol>
   *   <li>Job-level {@code entityTypes} when non-empty (explicit override for all passes)
   *   <li>When orphan is due: {@code systemMetadataFilterConfig.entityTypes} if set, else all
   *       registry types
   *   <li>Otherwise: entity types with registered default checks
   * </ol>
   */
  @Nonnull
  public Set<String> resolveEntityTypes(boolean orphanDue) {
    List<String> jobEntityTypes = config.getEntityTypes();
    if (jobEntityTypes != null && !jobEntityTypes.isEmpty()) {
      return new LinkedHashSet<>(jobEntityTypes);
    }
    if (orphanDue) {
      SystemMetadataFilterConfig smFilter = config.getSystemMetadataFilterConfig();
      if (smFilter != null && smFilter.hasEntityTypes()) {
        return new LinkedHashSet<>(smFilter.getEntityTypes());
      }
      return opContext.getEntityRegistry().getEntitySpecs().keySet().stream()
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    // Default to all entity types that have registered checks
    return consistencyService.getCheckRegistry().getDefaultEntityTypes();
  }

  /** Log issues found. */
  private void logIssues(List<ConsistencyIssue> issues) {
    for (ConsistencyIssue issue : issues) {
      boolean apply = config.shouldApplyFixes(issue.getCheckId());
      log.info(
          "[{}] {} entity={} check={} description={}",
          apply ? "APPLY" : "DRY-RUN",
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
    if (isReprocessEnabled()) {
      log.info("{}: Reprocess enabled, not skipping.", getUpgradeIdUrn());
      return false;
    }

    // Orphan cadence due → do not skip (full scan needed)
    if (isOrphanCheckDue(context)) {
      log.info("{}: Orphan check due, not skipping.", getUpgradeIdUrn());
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
