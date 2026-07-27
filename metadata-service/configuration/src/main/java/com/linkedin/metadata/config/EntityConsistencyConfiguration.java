package com.linkedin.metadata.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Data;

/**
 * Configuration for entity consistency checking and fixing.
 *
 * <p>Loaded from application.yaml under systemUpdate.entityConsistency
 *
 * <p><b>Fetch Strategy:</b> The service decides between entity search index and system metadata
 * index based on:
 *
 * <ul>
 *   <li>If any check has a targetAspect → use system metadata index for aspect filtering
 *   <li>If systemMetadataFilterConfig has timestamps configured → use system metadata index
 *   <li>Otherwise → use entity search index
 * </ul>
 *
 * <p><b>Per-check mode:</b> {@link #checks} controls whether each check is {@code disabled}, {@code
 * dry-run}, or {@code active}. Job-level {@link #dryRun} is a safety ceiling — when true, no check
 * writes regardless of per-check mode.
 */
@Data
public class EntityConsistencyConfiguration {

  /** Check ID for orphaned system-metadata / search / graph index documents. */
  public static final String ORPHAN_INDEX_DOCUMENT_CHECK_ID = "orphan-index-document";

  /** Whether the consistency check is enabled */
  private boolean enabled;

  /**
   * Job-level dry-run ceiling. When true, no check applies fixes. When false, each check's {@link
   * CheckRunConfig#mode} controls whether fixes are applied.
   */
  private boolean dryRun;

  /** Batch size for scanning entities */
  private int batchSize;

  /** Delay between batches in milliseconds */
  private int delayMs;

  /**
   * Grace period in seconds. Entities modified within this window are excluded from checks to avoid
   * false positives from eventual consistency race conditions. Default: 4 hours (14400 seconds).
   */
  private long gracePeriodSeconds;

  /** Maximum number of entities to process (0 for unlimited) */
  private int limit;

  /**
   * Job-level entity types for default (non-orphan) checks. If empty or null, uses entity types
   * that have registered default checks.
   *
   * <p>System-metadata discovery scope (orphan / SM-filtered scans) uses {@link
   * SystemMetadataFilterConfig#entityTypes} instead.
   */
  private List<String> entityTypes;

  /**
   * Specific check IDs to run. If empty or null, runs all default (non on-demand) checks for the
   * configured entity types. Example: ["assertion-entity-not-found", "monitor-assertions-empty"]
   */
  private List<String> checkIds;

  /** Reprocess configuration */
  private Reprocess reprocess;

  /**
   * Per-check upgrade configuration keyed by check ID. Controls mode (disabled / dry-run / active)
   * and optional cadence schedule for datahub-upgrade runs.
   */
  private Map<String, CheckRunConfig> checks;

  /**
   * System metadata filter configuration - when specified, uses system metadata index for entity
   * selection with timestamp and aspect filtering.
   */
  private SystemMetadataFilterConfig systemMetadataFilterConfig;

  @Data
  public static class Reprocess {
    /** Whether to reprocess even if previously completed */
    private boolean enabled;
  }

  /**
   * Per-check configuration for the entity-consistency upgrade job.
   *
   * <p>Mode defaults to {@link ConsistencyCheckMode#DRY_RUN} when unset. Schedule defaults to
   * {@link ConsistencyCheckSchedule#EVERY_RUN} when unset (except yaml defaults for specific checks
   * such as orphan-index-document).
   */
  @Data
  public static class CheckRunConfig {
    /** disabled | dry-run | active */
    private String mode;

    /** every-run | daily | weekly | monthly */
    private String schedule;

    @Nonnull
    public ConsistencyCheckMode resolvedMode() {
      return ConsistencyCheckMode.fromString(mode);
    }

    @Nonnull
    public ConsistencyCheckSchedule resolvedSchedule() {
      return ConsistencyCheckSchedule.fromString(schedule);
    }
  }

  /**
   * Resolve per-check config, returning an empty config (dry-run / every-run defaults) when absent.
   */
  @Nonnull
  public CheckRunConfig getCheckRunConfig(@Nonnull String checkId) {
    if (checks == null) {
      return new CheckRunConfig();
    }
    CheckRunConfig config = checks.get(checkId);
    return config != null ? config : new CheckRunConfig();
  }

  /** Resolved mode for a check ID. */
  @Nonnull
  public ConsistencyCheckMode getCheckMode(@Nonnull String checkId) {
    return getCheckRunConfig(checkId).resolvedMode();
  }

  /** Resolved schedule for a check ID. */
  @Nonnull
  public ConsistencyCheckSchedule getCheckSchedule(@Nonnull String checkId) {
    return getCheckRunConfig(checkId).resolvedSchedule();
  }

  /**
   * Whether fixes should be applied for a check, respecting the job-level dry-run ceiling.
   *
   * @return true only when job dryRun is false and the check mode is active
   */
  public boolean shouldApplyFixes(@Nonnull String checkId) {
    if (dryRun) {
      return false;
    }
    return getCheckMode(checkId).shouldApplyFixes();
  }

  @Nonnull
  public Map<String, CheckRunConfig> getChecksOrEmpty() {
    return checks != null ? checks : Collections.emptyMap();
  }

  /**
   * Configuration for system metadata index-based entity filtering.
   *
   * <p>When any of these parameters are set, the service will use the system metadata index instead
   * of the entity search index. This enables:
   *
   * <ul>
   *   <li>Processing only entities modified within a specific time range (via aspectModifiedTime or
   *       aspectCreatedTime)
   *   <li>Processing entities that have a specific aspect
   * </ul>
   */
  @Data
  public static class SystemMetadataFilterConfig {
    /**
     * Only include entities modified at or after this timestamp (epoch milliseconds).
     *
     * <p>Uses aspectModifiedTime (preferred) with aspectCreatedTime as fallback.
     */
    private Long gePitEpochMs;

    /**
     * Only include entities modified at or before this timestamp (epoch milliseconds).
     *
     * <p>Uses aspectModifiedTime (preferred) with aspectCreatedTime as fallback.
     */
    private Long lePitEpochMs;

    /**
     * Aspect name filters - only include entities that have ANY of these aspects.
     *
     * <p>This filters at the system metadata index level before fetching entity data. If multiple
     * aspects are specified, entities with ANY of the aspects will be included (OR semantics).
     */
    private List<String> aspectFilters;

    /**
     * Whether to include soft-deleted entities in the results.
     *
     * <p>Defaults to false (exclude soft-deleted entities). When true, entities with
     * Status.removed=true will be included in the consistency check.
     */
    private Boolean includeSoftDeleted;

    /**
     * When true, scroll only system-metadata documents for each entity type's key aspect. Reduces
     * work for orphan and large-type scans. Defaults to false when unset; upgrade job yaml enables
     * this by default.
     */
    private Boolean keyAspectOnly;

    /**
     * Entity types to include in system-metadata discovery (orphan scans and other SM-filtered
     * passes). Empty or null = all registry entity types.
     */
    private List<String> entityTypes;

    /**
     * Check if any filter parameters are configured.
     *
     * @return true if system metadata filtering should be used
     */
    public boolean hasAnyConfig() {
      return gePitEpochMs != null
          || lePitEpochMs != null
          || (aspectFilters != null && !aspectFilters.isEmpty())
          || Boolean.TRUE.equals(keyAspectOnly)
          || (entityTypes != null && !entityTypes.isEmpty());
    }

    /** Whether this filter restricts discovery to a configured entity-type list. */
    public boolean hasEntityTypes() {
      return entityTypes != null && !entityTypes.isEmpty();
    }

    /**
     * Check if soft-deleted entities should be included.
     *
     * @return true if soft-deleted entities should be included, false by default
     */
    public boolean isIncludeSoftDeleted() {
      return Boolean.TRUE.equals(includeSoftDeleted);
    }

    /** Whether discovery should use key-aspect system-metadata documents only. */
    public boolean isKeyAspectOnly() {
      return Boolean.TRUE.equals(keyAspectOnly);
    }
  }

  /**
   * Check if system metadata filtering should be used based on configuration.
   *
   * @return true if systemMetadataFilterConfig has any parameters set
   */
  public boolean useSystemMetadataFilter() {
    return systemMetadataFilterConfig != null && systemMetadataFilterConfig.hasAnyConfig();
  }
}
