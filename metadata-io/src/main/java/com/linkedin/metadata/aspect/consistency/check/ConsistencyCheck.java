package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Interface for consistency checks that validate invariants for entities.
 *
 * <p>Each implementation represents a specific consistency invariant that can be checked.
 *
 * <p>Implementations should be stateless and thread-safe. All state needed for checking should be
 * passed via the {@link CheckContext}.
 *
 * <p>Checks operate on batches of entities for efficiency. The {@link #check} method receives a map
 * of URNs to their entity responses and returns all issues found across the batch.
 *
 * <p>Each {@link ConsistencyIssue} returned by a check must specify its own fix type and include
 * all data needed for the fix to be applied (batchItems or hardDeleteUrns).
 *
 * <p><b>Entity Selection:</b> Checks declare their requirements:
 *
 * <ul>
 *   <li>{@link #getEntityType()} - Entity type to check (required, used as URN prefix filter)
 *   <li>{@link #getRequiredAspects()} - Aspects to fetch for each entity (required)
 *   <li>{@link #getTargetAspects()} - Aspects that entities must have (optional, for filtering)
 * </ul>
 *
 * <p>The service uses the system metadata index to find entities by type (URN prefix) and
 * optionally by aspect existence, then fetches entity data from SQL.
 */
public interface ConsistencyCheck {

  /**
   * Unique identifier for this check.
   *
   * <p>By default (when extending {@link AbstractEntityCheck}), the ID is derived from the class
   * name by removing the "Check" suffix and converting from PascalCase to kebab-case. For example:
   *
   * <ul>
   *   <li>AssertionEntityUrnMissingCheck → assertion-entity-urn-missing
   *   <li>MonitorAssertionsEmptyCheck → monitor-assertions-empty
   * </ul>
   *
   * @return unique check identifier
   */
  @Nonnull
  String getId();

  /**
   * Human-readable name for this check.
   *
   * @return display name
   */
  @Nonnull
  String getName();

  /**
   * Detailed description of what this check validates.
   *
   * @return description
   */
  @Nonnull
  String getDescription();

  /**
   * Entity type this check applies to.
   *
   * <p>Used for grouping checks by entity type and determining which entities to fetch.
   *
   * @return entity type (e.g., "assertion", "monitor")
   */
  @Nonnull
  String getEntityType();

  /**
   * Aspects to fetch for each entity when running this check.
   *
   * <p>These aspects will be included in the {@link EntityResponse} passed to {@link #check}. The
   * service consolidates required aspects across all checks being run together.
   *
   * <p>Return values:
   *
   * <ul>
   *   <li>{@code Optional.of(Set.of("aspect1", "aspect2"))} - fetch specific aspects
   *   <li>{@code Optional.of(Set.of())} - no extra aspects needed (only status for soft-delete
   *       tracking)
   *   <li>{@code Optional.empty()} - fetch ALL aspects (for checks like schema validation that
   *       examine all aspects present on an entity)
   * </ul>
   *
   * @return optional set of aspect names to fetch, or empty Optional for all aspects
   */
  @Nonnull
  Optional<Set<String>> getRequiredAspects();

  /**
   * Target aspects for entity selection (optional).
   *
   * <p>When specified, only entities that have ALL of these aspects will be selected for checking.
   * This filters at the system metadata index level before fetching entity data.
   *
   * <p>Use this when you need to check entities based on the presence of specific aspects rather
   * than all entities of a type.
   *
   * @return set of aspect names for filtering, or empty set for all entities of the type
   */
  @Nonnull
  default Set<String> getTargetAspects() {
    return Set.of();
  }

  /**
   * Perform the consistency check on a batch of entities.
   *
   * <p>The check should examine each entity in the batch and return an Issue for any consistency
   * violations found. Each Issue must specify its fix type and include all data needed for the fix
   * to be applied (batchItems for MCP-based fixes, hardDeleteUrns for entity deletions).
   *
   * @param ctx shared context containing services, caches, and collected state
   * @param entityResponses map of URN to entity response for the batch
   * @return list of issues found across all entities in the batch (may be empty)
   */
  @Nonnull
  List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses);

  /**
   * Whether this check is "on-demand only".
   *
   * <p>On-demand checks are excluded from default/wildcard runs in the upgrade job and API
   * endpoints. They can only be invoked by explicitly specifying their check ID.
   *
   * <p>Use this for checks that:
   *
   * <ul>
   *   <li>Have side effects that should not run automatically (e.g., creating entities)
   *   <li>Are expensive and should only be run when explicitly requested
   *   <li>Are intended for specific remediation scenarios
   * </ul>
   *
   * @return true if this check is on-demand only, false otherwise (default)
   */
  default boolean isOnDemandOnly() {
    return false;
  }
}
