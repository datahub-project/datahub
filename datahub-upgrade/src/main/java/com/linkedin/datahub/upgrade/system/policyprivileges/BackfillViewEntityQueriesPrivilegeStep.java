package com.linkedin.datahub.upgrade.system.policyprivileges;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;

/**
 * One-time upgrade step that appends {@code VIEW_ENTITY_QUERIES} to every existing policy that
 * grants {@code VIEW_ENTITY_PAGE} or {@code VIEW_DATASET_USAGE} but not {@code
 * VIEW_ENTITY_QUERIES}.
 *
 * <p>Before {@code VIEW_ENTITY_QUERIES} existed, query visibility was implied by entity-page
 * visibility, so this backfill is exactly behavior-preserving for existing installs: everyone who
 * could see queries before the upgrade can still see them after, and administrators can then revoke
 * the privilege deliberately. Fresh installs get the privilege via the default policies in {@code
 * policies.json} instead.
 *
 * <p>{@code VIEW_DATASET_USAGE} is also a triggering privilege: pre-upgrade, holding it alone (with
 * no {@code VIEW_ENTITY_PAGE}) was enough to see a dataset's usage stats including {@code
 * topSqlQueries}, since query-SQL gating didn't exist yet. Without this, such a usage-only policy
 * would silently lose {@code topSqlQueries} access on upgrade. Backfilling {@code
 * VIEW_ENTITY_QUERIES} onto it is not perfectly surgical — it also grants visibility into other
 * query-derived content for the same datasets (Query entities, view definitions, transform logic) —
 * but that is the same compatibility trade-off the {@code VIEW_ENTITY_PAGE} trigger already makes;
 * see {@code docs/how/updating-datahub.md}.
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
        batchSize,
        /* restrictToSystemPoliciesWhenViewAuthEnabled= */ false,
        /* alsoTriggerOnViewDatasetUsage= */ true);
  }

  @VisibleForTesting
  static boolean shouldBackfill(DataHubPolicyInfo info) {
    return AbstractBackfillQueryPrivilegeStep.shouldBackfill(
        info,
        VIEW_ENTITY_QUERIES,
        /* restrictToSystemPolicies= */ false,
        /* alsoTriggerOnViewDatasetUsage= */ true);
  }
}
