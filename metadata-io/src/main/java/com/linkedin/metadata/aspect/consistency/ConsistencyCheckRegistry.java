package com.linkedin.metadata.aspect.consistency;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.aspect.consistency.check.ConsistencyCheck;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry for consistency checks.
 *
 * <p>Collects all registered {@link ConsistencyCheck} implementations and provides methods to query
 * them by ID or entity type.
 *
 * <p><b>Wildcard Entity Type:</b> Checks with entity type "*" (wildcard) can be applied to any
 * entity type. When querying by entity type, wildcard checks are automatically included alongside
 * entity-specific checks. The {@code isOnDemandOnly()} flag is still respected when using default
 * check methods.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. All internal collections are immutable after
 * construction.
 */
@Slf4j
@ThreadSafe
public class ConsistencyCheckRegistry {

  /** Wildcard entity type - checks with this type can apply to any entity */
  public static final String WILDCARD_ENTITY_TYPE = "*";

  private final ImmutableMap<String, ConsistencyCheck> checksById;
  private final ImmutableList<ConsistencyCheck> allChecks;
  private final ImmutableMap<String, ImmutableList<ConsistencyCheck>> checksByEntityType;

  /**
   * Create a new registry with the given checks.
   *
   * @param checks list of consistency checks to register
   */
  public ConsistencyCheckRegistry(@Nonnull Collection<ConsistencyCheck> checks) {
    // Build checksById map preserving insertion order
    Map<String, ConsistencyCheck> byIdBuilder = new LinkedHashMap<>();
    for (ConsistencyCheck check : checks) {
      if (byIdBuilder.containsKey(check.getId())) {
        log.warn(
            "Duplicate check ID '{}', overwriting with {}",
            check.getId(),
            check.getClass().getSimpleName());
      }
      byIdBuilder.put(check.getId(), check);
      log.info("Registered consistency check: {} ({})", check.getId(), check.getName());
    }
    this.checksById = ImmutableMap.copyOf(byIdBuilder);
    this.allChecks = ImmutableList.copyOf(byIdBuilder.values());

    // Pre-compute checks by entity type for efficient lookup
    Map<String, ImmutableList<ConsistencyCheck>> byTypeBuilder =
        allChecks.stream()
            .collect(
                Collectors.groupingBy(
                    ConsistencyCheck::getEntityType,
                    Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf)));
    this.checksByEntityType = ImmutableMap.copyOf(byTypeBuilder);

    log.info("ConsistencyCheckRegistry initialized with {} checks", allChecks.size());
  }

  /**
   * Get all registered checks.
   *
   * @return unmodifiable list of all checks in registration order
   */
  @Nonnull
  public List<ConsistencyCheck> getAll() {
    return allChecks;
  }

  /**
   * Get a check by its ID.
   *
   * @param id check ID
   * @return optional containing the check if found
   */
  @Nonnull
  public Optional<ConsistencyCheck> getById(@Nonnull String id) {
    return Optional.ofNullable(checksById.get(id));
  }

  /**
   * Get all checks that apply to a specific entity type, including wildcard checks.
   *
   * <p>Returns both entity-specific checks and wildcard checks ("*" entity type) since wildcard
   * checks can apply to any entity type.
   *
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @return list of checks for the entity type plus wildcard checks (empty if none found)
   */
  @Nonnull
  public List<ConsistencyCheck> getByEntityType(@Nonnull String entityType) {
    List<ConsistencyCheck> typeChecks =
        checksByEntityType.getOrDefault(entityType, ImmutableList.of());

    // If querying for wildcard itself, just return the wildcard checks
    if (WILDCARD_ENTITY_TYPE.equals(entityType)) {
      return typeChecks;
    }

    // Include wildcard checks alongside entity-specific checks
    List<ConsistencyCheck> wildcardChecks =
        checksByEntityType.getOrDefault(WILDCARD_ENTITY_TYPE, ImmutableList.of());

    if (wildcardChecks.isEmpty()) {
      return typeChecks;
    }

    return Stream.concat(typeChecks.stream(), wildcardChecks.stream()).collect(Collectors.toList());
  }

  /**
   * Get checks by entity type only (excluding wildcard checks).
   *
   * <p>This is useful when you need only checks that are specifically registered for an entity type
   * without including wildcard checks.
   *
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @return immutable list of checks for the exact entity type (empty if none found)
   */
  @Nonnull
  public List<ConsistencyCheck> getByEntityTypeExact(@Nonnull String entityType) {
    return checksByEntityType.getOrDefault(entityType, ImmutableList.of());
  }

  /**
   * Get checks by IDs.
   *
   * @param checkIds check IDs to get
   * @return list of checks with matching IDs (in the same order as input)
   */
  @Nonnull
  public List<ConsistencyCheck> getByIds(@Nonnull Collection<String> checkIds) {
    return checkIds.stream()
        .map(checksById::get)
        .filter(check -> check != null)
        .collect(Collectors.toList());
  }

  /**
   * Get checks for a specific entity type, filtered by check IDs.
   *
   * <p>When checkIds are explicitly specified, this method filters both entity-specific and
   * wildcard checks by the requested IDs.
   *
   * @param entityType entity type
   * @param checkIds optional check IDs to filter by (if empty or null, returns all for entity type
   *     including wildcards)
   * @return list of matching checks
   */
  @Nonnull
  public List<ConsistencyCheck> getByEntityTypeAndIds(
      @Nonnull String entityType, @Nullable Collection<String> checkIds) {
    // getByEntityType already includes wildcard checks
    List<ConsistencyCheck> allApplicableChecks = getByEntityType(entityType);
    if (checkIds == null || checkIds.isEmpty()) {
      return allApplicableChecks;
    }

    Set<String> idSet = checkIds instanceof Set ? (Set<String>) checkIds : Set.copyOf(checkIds);

    // Filter by the specified IDs
    return allApplicableChecks.stream()
        .filter(c -> idSet.contains(c.getId()))
        .collect(Collectors.toList());
  }

  /**
   * Get all entity types that have registered checks.
   *
   * <p>Includes the wildcard entity type ("*") if any wildcard checks are registered.
   *
   * @return set of entity types
   */
  @Nonnull
  public Set<String> getEntityTypes() {
    return checksByEntityType.keySet();
  }

  /**
   * Get all check IDs.
   *
   * @return immutable set of check IDs
   */
  @Nonnull
  public Set<String> getCheckIds() {
    return checksById.keySet();
  }

  /**
   * Get the number of registered checks.
   *
   * @return count of checks
   */
  public int size() {
    return allChecks.size();
  }

  // ============================================================================
  // Default checks (excluding on-demand only)
  // ============================================================================

  /**
   * Get all default checks (excluding on-demand only checks).
   *
   * <p>Default checks are those that should run during wildcard/default runs in upgrade jobs and
   * API endpoints.
   *
   * @return list of default checks
   */
  @Nonnull
  public List<ConsistencyCheck> getDefaultChecks() {
    return allChecks.stream().filter(check -> !check.isOnDemandOnly()).collect(Collectors.toList());
  }

  /**
   * Get default checks for a specific entity type, including wildcard default checks.
   *
   * <p>Returns both entity-specific and wildcard checks, but only those that are not on-demand
   * only. Wildcard checks that have {@code isOnDemandOnly() == true} are excluded.
   *
   * @param entityType entity type (e.g., "assertion", "monitor")
   * @return list of default checks for the entity type plus wildcard default checks
   */
  @Nonnull
  public List<ConsistencyCheck> getDefaultByEntityType(@Nonnull String entityType) {
    return getByEntityType(entityType).stream()
        .filter(check -> !check.isOnDemandOnly())
        .collect(Collectors.toList());
  }

  /**
   * Get default checks for a specific entity type, optionally filtered by check IDs.
   *
   * <p>When checkIds is null or empty, returns only default checks (excludes on-demand). When
   * checkIds is specified, returns matching checks regardless of on-demand status.
   *
   * @param entityType entity type
   * @param checkIds optional check IDs to filter by (if specified, on-demand checks can be
   *     included)
   * @return list of matching checks
   */
  @Nonnull
  public List<ConsistencyCheck> getDefaultByEntityTypeAndIds(
      @Nonnull String entityType, @Nullable Collection<String> checkIds) {
    if (checkIds != null && !checkIds.isEmpty()) {
      // Explicit check IDs - include on-demand if specified
      return getByEntityTypeAndIds(entityType, checkIds);
    }
    // Wildcard - exclude on-demand checks
    return getDefaultByEntityType(entityType);
  }

  /**
   * Get all entity types that have at least one default check (non on-demand).
   *
   * <p>Excludes the wildcard entity type ("*") as it is not a real entity type.
   *
   * @return set of entity types with default checks
   */
  @Nonnull
  public Set<String> getDefaultEntityTypes() {
    return checksByEntityType.entrySet().stream()
        .filter(entry -> !WILDCARD_ENTITY_TYPE.equals(entry.getKey()))
        .filter(entry -> entry.getValue().stream().anyMatch(c -> !c.isOnDemandOnly()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }
}
