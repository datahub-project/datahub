package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Check that identifies orphaned index documents.
 *
 * <p>Orphaned documents are entities that exist in Elasticsearch indices (system metadata, entity
 * search, graph) but NOT in SQL. This typically happens when:
 *
 * <ul>
 *   <li>An entity was hard-deleted directly from SQL (bypassing proper cleanup)
 *   <li>A hard delete operation partially failed (SQL deleted, but ES cleanup skipped)
 *   <li>Race conditions during deletion
 * </ul>
 *
 * <p>{@link #check(CheckContext, Map)} does not see orphan URNs, because those entities are not in
 * the SQL batch. {@link #followUp} reports them from the orphan URNs the service already computed.
 *
 * <p>The fix is {@link ConsistencyFixType#DELETE_INDEX_DOCUMENTS} which removes the orphaned
 * documents from all indices without going through MCP.
 *
 * <p>This check applies to ALL entity types - it examines orphan URNs for whatever entity type is
 * being processed.
 */
@Slf4j
@Component("consistencyOrphanIndexDocumentCheck")
public class OrphanIndexDocumentCheck implements ConsistencyCheck {

  public static final String CHECK_ID = "orphan-index-document";

  @Override
  @Nonnull
  public String getId() {
    return CHECK_ID;
  }

  @Override
  @Nonnull
  public String getName() {
    return "Orphan Index Document";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Detects entities that exist in ES indices but not in SQL (orphaned index documents)";
  }

  /**
   * This check applies to ALL entity types.
   *
   * <p>Returns the wildcard entity type "*" which indicates this check applies universally.
   */
  @Override
  @Nonnull
  public String getEntityType() {
    return ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    // No aspects needed - we're checking for entities that DON'T exist in SQL
    return Optional.of(Set.of());
  }

  /** SQL entities are not orphans. Orphan URNs are reported from {@link #followUp}. */
  @Override
  @Nonnull
  public List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
    return List.of();
  }

  @Override
  @Nonnull
  public CheckFollowUp followUp(@Nonnull CheckFollowUpRequest request) {
    // The request set is this page. The context set accumulates across batches when a caller
    // reuses it, so reading the context would report orphans that were already handled.
    List<ConsistencyIssue> issues = issuesFor(request.getOrphanUrns(), request.getEntityType());
    if (issues.isEmpty()) {
      return CheckFollowUp.none();
    }
    return CheckFollowUp.builder().issues(issues).build();
  }

  /**
   * Check for orphan URNs of a specific entity type.
   *
   * <p>The batch context holds URNs the service found in the system-metadata page and not in SQL.
   *
   * @param ctx the check context containing orphan URNs
   * @param entityType the entity type to check for orphans
   * @return list of issues for orphaned URNs
   */
  @Nonnull
  public List<ConsistencyIssue> checkOrphans(
      @Nonnull CheckContext ctx, @Nonnull String entityType) {
    return issuesFor(ctx.getOrphanUrns(entityType), entityType);
  }

  @Nonnull
  private List<ConsistencyIssue> issuesFor(
      @Nonnull Set<Urn> orphanUrns, @Nonnull String entityType) {
    if (orphanUrns.isEmpty()) {
      return List.of();
    }

    List<ConsistencyIssue> issues = new ArrayList<>();

    for (Urn orphanUrn : orphanUrns) {
      try {
        issues.add(
            ConsistencyIssue.builder()
                .entityUrn(orphanUrn)
                .entityType(entityType)
                .checkId(CHECK_ID)
                .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
                .description(
                    String.format(
                        "Entity exists in ES indices but not in SQL (orphaned): %s", orphanUrn))
                .hardDeleteUrns(List.of(orphanUrn))
                .build());

        log.debug("Found orphan index document: {} (type={})", orphanUrn, entityType);
      } catch (Exception e) {
        log.error(
            "Error creating issue for orphan URN {}: {}. Continuing with remaining URNs.",
            orphanUrn,
            e.getMessage(),
            e);
      }
    }

    if (!issues.isEmpty()) {
      log.info("Found {} orphan index documents for entity type {}", issues.size(), entityType);
    }

    return issues;
  }

  /**
   * This check is on-demand only.
   *
   * <p>Orphan detection requires comparing ES index contents against SQL, which can be expensive.
   * It should only be run when explicitly requested.
   *
   * @return true - only run when explicitly requested
   */
  @Override
  public boolean isOnDemandOnly() {
    return true;
  }
}
