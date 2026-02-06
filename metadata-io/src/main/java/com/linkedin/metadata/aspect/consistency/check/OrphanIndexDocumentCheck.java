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
 * <p>This check does NOT use the normal {@link #check(CheckContext, Map)} pattern that examines
 * EntityResponses. Instead, it examines the {@link CheckContext#getOrphanUrns(String)} which is
 * populated by ConsistencyService when URNs found in ES don't have corresponding SQL records.
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

  private static final String CHECK_ID = "orphan-index-document";

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

  /**
   * Normal check method - we override this but the real work happens in orphan detection.
   *
   * <p>This method is called with entityResponses that DO exist in SQL. The orphan URNs are the
   * ones that were queried from ES but NOT in this map. ConsistencyService populates the orphan
   * URNs in CheckContext before calling this.
   */
  @Override
  @Nonnull
  public List<ConsistencyIssue> check(
      @Nonnull CheckContext ctx, @Nonnull Map<Urn, EntityResponse> entityResponses) {
    // The actual orphan detection is done by examining ctx.getOrphanUrns()
    // This is called separately by ConsistencyService after populating orphan URNs
    return List.of();
  }

  /**
   * Check for orphan URNs of a specific entity type.
   *
   * <p>This method is called directly by ConsistencyService after it identifies orphan URNs.
   *
   * @param ctx the check context containing orphan URNs
   * @param entityType the entity type to check for orphans
   * @return list of issues for orphaned URNs
   */
  @Nonnull
  public List<ConsistencyIssue> checkOrphans(
      @Nonnull CheckContext ctx, @Nonnull String entityType) {
    Set<Urn> orphanUrns = ctx.getOrphanUrns(entityType);
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
