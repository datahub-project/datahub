package com.linkedin.metadata.aspect.consistency;

import com.linkedin.metadata.config.EntityConsistencyConfiguration;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

/**
 * Filter configuration for querying entities via the system metadata index.
 *
 * <p>This class is used by {@link ConsistencyService} to filter entities based on:
 *
 * <ul>
 *   <li>Timestamp ranges (aspectModifiedTime/aspectCreatedTime)
 *   <li>Aspect existence (entities that have specific aspects)
 *   <li>Soft-delete status
 * </ul>
 *
 * <p>This is a service-layer class independent of YAML configuration. Callers (API controllers,
 * upgrade jobs) should construct this from their own configuration sources.
 */
@Data
@Builder
public class SystemMetadataFilter {

  /**
   * Convert from YAML configuration class to service-layer filter.
   *
   * @param config YAML configuration (may be null)
   * @return service-layer filter (null if config is null)
   */
  @Nullable
  public static SystemMetadataFilter from(
      @Nullable EntityConsistencyConfiguration.SystemMetadataFilterConfig config) {
    if (config == null) {
      return null;
    }
    return SystemMetadataFilter.builder()
        .gePitEpochMs(config.getGePitEpochMs())
        .lePitEpochMs(config.getLePitEpochMs())
        .aspectFilters(config.getAspectFilters())
        .includeSoftDeleted(config.isIncludeSoftDeleted())
        .build();
  }

  /**
   * Only include entities modified at or after this timestamp (epoch milliseconds).
   *
   * <p>Uses aspectModifiedTime (preferred) with aspectCreatedTime as fallback.
   */
  @Nullable private final Long gePitEpochMs;

  /**
   * Only include entities modified at or before this timestamp (epoch milliseconds).
   *
   * <p>Uses aspectModifiedTime (preferred) with aspectCreatedTime as fallback.
   */
  @Nullable private final Long lePitEpochMs;

  /**
   * Aspect name filters - only include entities that have ANY of these aspects.
   *
   * <p>This filters at the system metadata index level before fetching entity data. If multiple
   * aspects are specified, entities with ANY of the aspects will be included (OR semantics).
   */
  @Nullable private final List<String> aspectFilters;

  /**
   * Whether to include soft-deleted entities in the results.
   *
   * <p>Defaults to false (exclude soft-deleted entities). When true, entities with
   * Status.removed=true will be included in the consistency check.
   */
  private final boolean includeSoftDeleted;

  /**
   * Check if any filter parameters are configured.
   *
   * @return true if any filtering is enabled
   */
  public boolean hasAnyConfig() {
    return gePitEpochMs != null
        || lePitEpochMs != null
        || (aspectFilters != null && !aspectFilters.isEmpty());
  }

  /** Create an empty filter with default values. */
  public static SystemMetadataFilter empty() {
    return SystemMetadataFilter.builder().includeSoftDeleted(false).build();
  }
}
