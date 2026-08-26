package com.linkedin.datahub.upgrade.system.policyprivileges;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;

/**
 * One-time upgrade step that appends {@code VIEW_ALL_QUERIES} to every existing policy that grants
 * {@code VIEW_ENTITY_PAGE} but not {@code VIEW_ALL_QUERIES}.
 *
 * <p><b>{@code VIEW_ALL_QUERIES} is a distinct, platform-level privilege — NOT a broader version of
 * {@code VIEW_ENTITY_QUERIES}.</b> {@code VIEW_ENTITY_QUERIES} is resource-scoped (checked against
 * a query's subject datasets) and is what {@link BackfillViewEntityQueriesPrivilegeStep} backfills.
 * {@code VIEW_ALL_QUERIES} instead grants unconditional visibility into every query's SQL text,
 * subjects or not, with no per-dataset restriction — see {@link
 * com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils#filterViewableQueryEntities},
 * which checks it first and short-circuits the subject-derived logic entirely when it is held. A
 * query with no recorded {@code querySubjects} aspect is the one case {@code VIEW_ENTITY_QUERIES}
 * can never reach on its own — it has nothing to check that resource-scoped privilege against and
 * is fail-closed to every actor, including root/admin, regardless of policy breadth — but {@code
 * VIEW_ALL_QUERIES} is not limited to that case.
 *
 * <p>Before either privilege existed, being able to view an entity's page implied being able to
 * view ALL of its queries, subjects or not — so this backfill targets the same actor set as {@link
 * BackfillViewEntityQueriesPrivilegeStep} for full behavior parity across the upgrade.
 * Administrators may subsequently want to review and narrow who holds {@code VIEW_ALL_QUERIES}:
 * granting it broadly (to "most users", mirroring pre-upgrade behavior) is not the same posture as
 * a properly least-privilege deployment, where ordinary users hold only the dataset-scoped {@code
 * VIEW_ENTITY_QUERIES} and {@code VIEW_ALL_QUERIES} is reserved for admin/reader-tier roles that
 * genuinely need unconditional visibility into every query, not just orphaned ones.
 *
 * <p>Scrolling, batching, and policy-mutation logic live in {@link
 * AbstractBackfillQueryPrivilegeStep}, shared with {@link BackfillViewEntityQueriesPrivilegeStep}.
 */
public class BackfillViewAllQueriesPrivilegeStep extends AbstractBackfillQueryPrivilegeStep {
  private static final String UPGRADE_ID = "BackfillViewAllQueriesPrivilegeStep";
  private static final String VIEW_ALL_QUERIES =
      PoliciesConfig.VIEW_ALL_QUERIES_PRIVILEGE.getType();

  public BackfillViewAllQueriesPrivilegeStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    super(
        UPGRADE_ID,
        VIEW_ALL_QUERIES,
        opContext,
        entityService,
        searchService,
        reprocessEnabled,
        batchSize);
  }

  @VisibleForTesting
  static boolean shouldBackfill(DataHubPolicyInfo info) {
    return AbstractBackfillQueryPrivilegeStep.shouldBackfill(info, VIEW_ALL_QUERIES);
  }
}
