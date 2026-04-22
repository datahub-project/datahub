package com.linkedin.metadata.config;

import java.util.List;
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
 */
@Data
public class EntityConsistencyConfiguration {

  /** Whether the consistency check is enabled */
  private boolean enabled;

  /** Whether to run in dry-run mode (report only, no changes) */
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

  /** Entity types to check. If empty or null, uses all entity types that have registered checks. */
  private List<String> entityTypes;

  /**
   * Specific check IDs to run. If empty or null, runs all default (non on-demand) checks for the
   * configured entity types. Example: ["assertion-entity-not-found", "monitor-assertions-empty"]
   */
  private List<String> checkIds;

  /** Reprocess configuration */
  private Reprocess reprocess;

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
     * Check if any filter parameters are configured.
     *
     * @return true if system metadata filtering should be used
     */
    public boolean hasAnyConfig() {
      return gePitEpochMs != null
          || lePitEpochMs != null
          || (aspectFilters != null && !aspectFilters.isEmpty());
    }

    /**
     * Check if soft-deleted entities should be included.
     *
     * @return true if soft-deleted entities should be included, false by default
     */
    public boolean isIncludeSoftDeleted() {
      return Boolean.TRUE.equals(includeSoftDeleted);
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
