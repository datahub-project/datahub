package com.linkedin.datahub.upgrade.system.policyprivileges;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared scroll-all-policies, append-one-privilege-if-{@code VIEW_ENTITY_PAGE}-granted logic behind
 * {@link BackfillViewEntityQueriesPrivilegeStep} and {@link BackfillViewAllQueriesPrivilegeStep} —
 * the two backfills differ only in which privilege they append and their upgrade id; see each
 * subclass's javadoc for why the two privileges are backfilled to the same policies despite having
 * very different authorization semantics.
 *
 * <p>Modeled on {@link com.linkedin.datahub.upgrade.system.policyfields.BackfillPolicyFieldsStep}:
 * scrolls all policies in batches, is idempotent per policy, runs once (gated by a {@code
 * DataHubUpgradeResult} aspect), and may be re-run via the reprocess flag.
 */
@Slf4j
public abstract class AbstractBackfillQueryPrivilegeStep implements UpgradeStep {
  private static final String VIEW_ENTITY_PAGE =
      PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE.getType();

  /**
   * Each scroll page (sized by the configurable {@code batchSize}, which can run into the
   * thousands) is re-fetched via {@code getEntitiesV2} in chunks of this size, rather than one
   * {@code getEntityV2} call per urn, to avoid an N+1 read per page while keeping any single
   * multi-get call bounded.
   */
  private static final int ENTITY_FETCH_CHUNK_SIZE = 100;

  private final String upgradeId;
  private final Urn upgradeIdUrn;
  private final String targetPrivilege;
  private final OperationContext opContext;
  private final boolean reprocessEnabled;
  private final Integer batchSize;
  private final EntityService<?> entityService;
  private final SearchService searchService;

  protected AbstractBackfillQueryPrivilegeStep(
      String upgradeId,
      String targetPrivilege,
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.upgradeId = upgradeId;
    this.upgradeIdUrn = BootstrapStep.getUpgradeUrn(upgradeId);
    this.targetPrivilege = targetPrivilege;
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public String id() {
    return upgradeId;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      final AtomicInteger failureCount = new AtomicInteger(0);
      String scrollId = null;
      int migratedCount = 0;
      do {
        log.info(
            "Scanning batch of policies {}-{} for {} backfill",
            migratedCount,
            migratedCount + batchSize,
            targetPrivilege);
        scrollId = backfillPolicies(context, auditStamp, scrollId, failureCount);
        migratedCount += batchSize;
      } while (scrollId != null);

      if (failureCount.get() > 0) {
        log.error(
            "{} hit {} failure(s) backfilling {} -- leaving the upgrade marker unwritten so the"
                + " run is retried",
            id(),
            failureCount.get(),
            targetPrivilege);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      BootstrapStep.setUpgradeResult(context.opContext(), upgradeIdUrn, entityService);

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
            context.opContext(), upgradeIdUrn, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  private String backfillPolicies(
      UpgradeContext context, AuditStamp auditStamp, String scrollId, AtomicInteger failureCount) {
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

    final List<Urn> urns =
        scrollResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());

    for (List<Urn> chunk : Lists.partition(urns, ENTITY_FETCH_CHUNK_SIZE)) {
      final Map<Urn, EntityResponse> entityResponses;
      try {
        entityResponses =
            entityService.getEntitiesV2(
                context.opContext(),
                Constants.POLICY_ENTITY_NAME,
                new HashSet<>(chunk),
                Collections.singleton(DATAHUB_POLICY_INFO_ASPECT_NAME));
      } catch (Exception e) {
        // don't stop the whole step because one chunk's fetch failed
        log.error(
            String.format("Error fetching policies %s for %s backfill", chunk, targetPrivilege), e);
        failureCount.incrementAndGet();
        continue;
      }

      for (Urn urn : chunk) {
        try {
          backfillPolicy(context, urn, entityResponses.get(urn), auditStamp);
        } catch (Exception e) {
          // don't stop the whole step because of one bad urn or one bad ingestion
          log.error(String.format("Error backfilling %s for policy %s", targetPrivilege, urn), e);
          failureCount.incrementAndGet();
        }
      }
    }

    return scrollResult.getScrollId();
  }

  private void backfillPolicy(
      UpgradeContext context, Urn urn, EntityResponse entityResponse, AuditStamp auditStamp) {
    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      return;
    }

    final DataMap dataMap =
        entityResponse.getAspects().get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data();
    final DataHubPolicyInfo infoAspect = new DataHubPolicyInfo(dataMap);

    if (!shouldBackfill(infoAspect, targetPrivilege)) {
      return;
    }

    infoAspect.getPrivileges().add(targetPrivilege);
    log.info("Adding {} to policy {} (grants {})", targetPrivilege, urn, VIEW_ENTITY_PAGE);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(DATAHUB_POLICY_INFO_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(infoAspect));
    entityService.ingestProposal(context.opContext(), proposal, auditStamp, false);
  }

  protected static boolean shouldBackfill(DataHubPolicyInfo info, String targetPrivilege) {
    return info.hasPrivileges()
        && info.getPrivileges().contains(VIEW_ENTITY_PAGE)
        && !info.getPrivileges().contains(targetPrivilege);
  }
}
