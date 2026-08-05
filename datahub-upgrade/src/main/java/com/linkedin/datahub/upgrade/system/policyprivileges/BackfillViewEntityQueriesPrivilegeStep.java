package com.linkedin.datahub.upgrade.system.policyprivileges;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

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
 * <p>Modeled on {@link com.linkedin.datahub.upgrade.system.policyfields.BackfillPolicyFieldsStep}:
 * scrolls all policies in batches, is idempotent per policy, runs once (gated by a {@code
 * DataHubUpgradeResult} aspect), and may be re-run via the reprocess flag.
 */
@Slf4j
public class BackfillViewEntityQueriesPrivilegeStep implements UpgradeStep {
  private static final String UPGRADE_ID = "BackfillViewEntityQueriesPrivilegeStep";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private static final String VIEW_ENTITY_PAGE =
      PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType();
  private static final String VIEW_ENTITY_QUERIES =
      PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType();

  private final OperationContext opContext;
  private final boolean reprocessEnabled;
  private final Integer batchSize;
  private final EntityService<?> entityService;
  private final SearchService searchService;

  public BackfillViewEntityQueriesPrivilegeStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      String scrollId = null;
      int migratedCount = 0;
      do {
        log.info(
            "Scanning batch of policies {}-{} for {} backfill",
            migratedCount,
            migratedCount + batchSize,
            VIEW_ENTITY_QUERIES);
        scrollId = backfillPolicies(context, auditStamp, scrollId);
        migratedCount += batchSize;
      } while (scrollId != null);

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /** A failure after max retries should not block the rest of the upgrade. */
  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  private String backfillPolicies(UpgradeContext context, AuditStamp auditStamp, String scrollId) {
    // Policy counts are small; scroll everything and filter in code rather than depending on how
    // the privileges field happens to be indexed.
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(Constants.POLICY_ENTITY_NAME),
            "*",
            null,
            null,
            scrollId,
            null,
            batchSize);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return null;
    }

    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        backfillPolicy(context, searchEntity.getEntity(), auditStamp);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            String.format(
                "Error backfilling %s for policy %s",
                VIEW_ENTITY_QUERIES, searchEntity.getEntity()),
            e);
      }
    }

    return scrollResult.getScrollId();
  }

  private void backfillPolicy(UpgradeContext context, Urn urn, AuditStamp auditStamp)
      throws Exception {
    final EntityResponse entityResponse =
        entityService.getEntityV2(
            context.opContext(),
            urn.getEntityType(),
            urn,
            Collections.singleton(DATAHUB_POLICY_INFO_ASPECT_NAME));

    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      return;
    }

    final DataMap dataMap =
        entityResponse.getAspects().get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data();
    final DataHubPolicyInfo infoAspect = new DataHubPolicyInfo(dataMap);

    if (!shouldBackfill(infoAspect)) {
      return;
    }

    infoAspect.getPrivileges().add(VIEW_ENTITY_QUERIES);
    log.info("Adding {} to policy {} (grants {})", VIEW_ENTITY_QUERIES, urn, VIEW_ENTITY_PAGE);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(DATAHUB_POLICY_INFO_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(infoAspect));
    entityService.ingestProposal(context.opContext(), proposal, auditStamp, false);
  }

  @VisibleForTesting
  static boolean shouldBackfill(DataHubPolicyInfo info) {
    return info.hasPrivileges()
        && info.getPrivileges().contains(VIEW_ENTITY_PAGE)
        && !info.getPrivileges().contains(VIEW_ENTITY_QUERIES);
  }
}
