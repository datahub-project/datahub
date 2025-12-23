package com.linkedin.metadata.aspect.consistency.check.monitor;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.MonitorUrnUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Check that the monitor's entity matches the assertion's entityUrn.
 *
 * <p>For assertions on the same entity type, they should match exactly. For assertions on different
 * entity types that the monitor supports, the check handles the mismatch appropriately.
 *
 * <p>This check uses pre-fetched assertion info from the context cache when available, falling back
 * to individual lookups only when the data is not cached.
 */
@Slf4j
@Component("consistencyMonitorEntityUrnMismatchCheck")
public class MonitorEntityUrnMismatchCheck extends AbstractMonitorCheck {

  public MonitorEntityUrnMismatchCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Entity URN Mismatch";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the monitor's entity matches the assertion's entityUrn";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkMonitor(
      @Nonnull CheckContext ctx, @Nonnull Urn monitorUrn, @Nonnull EntityResponse response) {

    Urn monitorEntityUrn = MonitorUrnUtils.getEntityUrn(monitorUrn);
    if (monitorEntityUrn == null) {
      return Collections.emptyList();
    }

    // Skip if invalid entity type (handled by MonitorEntityTypeInvalidCheck)
    if (!isValidMonitorEntityType(monitorEntityUrn.getEntityType())) {
      return Collections.emptyList();
    }

    MonitorInfo monitorInfo = getMonitorInfo(response);
    if (monitorInfo == null) {
      return Collections.emptyList();
    }

    AssertionMonitor assertionMonitor = getAssertionMonitor(monitorInfo);
    if (assertionMonitor == null) {
      return Collections.emptyList();
    }

    // Skip if no assertions (handled by MonitorAssertionsEmptyCheck)
    if (!assertionMonitor.hasAssertions() || assertionMonitor.getAssertions().isEmpty()) {
      return Collections.emptyList();
    }

    // Check primary assertion
    AssertionEvaluationSpec primarySpec = assertionMonitor.getAssertions().get(0);
    if (!primarySpec.hasAssertion() || primarySpec.getAssertion() == null) {
      return Collections.emptyList();
    }

    Urn assertionUrn = primarySpec.getAssertion();

    // Skip if invalid entity type
    if (!ASSERTION_ENTITY_NAME.equals(assertionUrn.getEntityType())) {
      return Collections.emptyList();
    }

    // Skip if assertion is soft-deleted (handled by MonitorAssertionSoftDeletedCheck)
    if (isReferencedEntitySoftDeleted(ctx, assertionUrn)) {
      return Collections.emptyList();
    }

    // Get assertion's entityUrn from cache or fetch if not cached
    Urn assertionEntityUrn = getAssertionEntityUrn(ctx, assertionUrn);
    if (assertionEntityUrn == null) {
      return Collections.emptyList();
    }

    // Check match based on entity type
    boolean matches = checkEntityUrnMatch(monitorEntityUrn, assertionEntityUrn);

    if (!matches) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format(
                      "Monitor's entity (%s) doesn't match assertion's entityUrn (%s)",
                      monitorEntityUrn, assertionEntityUrn))
              .relatedUrns(List.of(assertionUrn))
              .details(assertionEntityUrn.toString())
              .batchItems(List.of(createSoftDeleteItem(ctx, monitorUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }

  /**
   * Get the entityUrn from an assertion, using the context cache when available.
   *
   * @param ctx check context with assertion info cache
   * @param assertionUrn assertion URN
   * @return entityUrn or null if not found
   */
  @Nullable
  private Urn getAssertionEntityUrn(CheckContext ctx, Urn assertionUrn) {
    // Try to get from cache first
    AssertionInfo cachedInfo = ctx.getCachedAspect(assertionUrn, ASSERTION_INFO_ASPECT_NAME);
    if (cachedInfo != null) {
      return cachedInfo.hasEntityUrn() ? cachedInfo.getEntityUrn() : null;
    }

    // Try to get from cached response
    EntityResponse cachedResponse = ctx.getCachedEntityResponse(assertionUrn);
    if (cachedResponse != null) {
      return extractEntityUrnFromResponse(cachedResponse);
    }

    // Not cached - return null (batch fetching should have populated the cache)
    log.debug("Assertion info not in cache for {}, skipping check", assertionUrn);
    return null;
  }

  /**
   * Extract entityUrn from an EntityResponse.
   *
   * @param response entity response
   * @return entityUrn or null
   */
  @Nullable
  private Urn extractEntityUrnFromResponse(EntityResponse response) {
    EnvelopedAspect infoAspect = response.getAspects().get(ASSERTION_INFO_ASPECT_NAME);
    if (infoAspect == null) {
      return null;
    }

    AssertionInfo assertionInfo = new AssertionInfo(infoAspect.getValue().data());
    if (!assertionInfo.hasEntityUrn()) {
      return null;
    }

    return assertionInfo.getEntityUrn();
  }

  /**
   * Check if the monitor's entity matches the assertion's entityUrn.
   *
   * <p>When the assertion references the same entity type as the monitor, the URNs must match
   * exactly. When they differ (e.g., monitor is for a dataset but assertion is for a dataJob), this
   * check is skipped since type validity is handled by other checks.
   *
   * @param monitorEntityUrn the monitor's entity URN
   * @param assertionEntityUrn the assertion's entity URN
   * @return true if they match or if entity types differ (skip check)
   */
  private boolean checkEntityUrnMatch(Urn monitorEntityUrn, Urn assertionEntityUrn) {
    // If same entity type, require exact match
    if (monitorEntityUrn.getEntityType().equals(assertionEntityUrn.getEntityType())) {
      return assertionEntityUrn.equals(monitorEntityUrn);
    }
    // Different entity types - skip this check
    // Type validity is handled by AssertionEntityTypeInvalidCheck
    return true;
  }
}
