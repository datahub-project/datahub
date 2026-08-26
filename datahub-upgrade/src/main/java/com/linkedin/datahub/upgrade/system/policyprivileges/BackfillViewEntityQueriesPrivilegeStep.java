package com.linkedin.datahub.upgrade.system.policyprivileges;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;

/**
 * One-time upgrade step that appends {@code VIEW_ENTITY_QUERIES} to every existing policy that
 * grants {@code VIEW_ENTITY_PAGE} but not {@code VIEW_ENTITY_QUERIES}.
 *
 * <p>Before {@code VIEW_ENTITY_QUERIES} existed, query visibility was implied by entity-page
 * visibility, so this backfill is exactly behavior-preserving for existing installs: everyone who
 * could see queries before the upgrade can still see them after, and administrators can then revoke
 * the privilege deliberately. Fresh installs get the privilege via the default policies in {@code
 * policies.json} instead.
 *
 * <p>Scrolling, batching, and policy-mutation logic live in {@link
 * AbstractBackfillQueryPrivilegeStep}, shared with {@link BackfillViewAllQueriesPrivilegeStep}.
 */
public class BackfillViewEntityQueriesPrivilegeStep extends AbstractBackfillQueryPrivilegeStep {
  private static final String UPGRADE_ID = "BackfillViewEntityQueriesPrivilegeStep";
  private static final String VIEW_ENTITY_QUERIES =
      PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType();

  public BackfillViewEntityQueriesPrivilegeStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    super(
        UPGRADE_ID,
        VIEW_ENTITY_QUERIES,
        opContext,
        entityService,
        searchService,
        reprocessEnabled,
        batchSize);
  }

  @VisibleForTesting
  static boolean shouldBackfill(DataHubPolicyInfo info) {
    return AbstractBackfillQueryPrivilegeStep.shouldBackfill(info, VIEW_ENTITY_QUERIES);
  }
}
