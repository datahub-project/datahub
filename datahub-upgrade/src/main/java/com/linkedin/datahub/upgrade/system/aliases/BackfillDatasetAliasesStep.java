package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Aliases;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AliasesUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Backfills the system-owned {@code aliases} aspect ({@code lowercasedUrn}) for datasets created
 * before {@code AliasesSideEffect} shipped; the side effect fires only on the dataset key aspect,
 * which is written once at creation.
 *
 * <p>Scans Elasticsearch for datasets whose {@code lowercasedUrn} is absent and emits an MCP for
 * each, leaving the writes to mce-consumer. The query is self-narrowing, so an interrupted run
 * resumes simply by running again — there is no checkpoint. The SUCCEEDED marker on {@code
 * urn:li:dataHubUpgrade:dataset-aliases-v1} gates case-insensitive resolution and is written when
 * the scroll is exhausted. A URN that cannot be lowercased is counted and skipped, since it can
 * never carry the aspect, but a failed emit ends the run: ingestion is asynchronous, so the only
 * error reachable here is the proposal not making it onto the topic at all.
 */
@Slf4j
public class BackfillDatasetAliasesStep implements UpgradeStep {

  /** The {@code -vN} suffix is the lowercasing-rule version; bump it when the rule changes. */
  public static final String UPGRADE_ID = AliasesUtils.DATASET_ALIASES_BACKFILL_UPGRADE_ID;

  private static final String LOWERCASED_URN_FIELD = "lowercasedUrn";
  private static final List<SortCriterion> URN_SORT =
      ImmutableList.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING));

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final int batchSize;
  private final long batchDelayMs;
  private final boolean reprocessEnabled;

  public BackfillDatasetAliasesStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull SearchService searchService,
      int batchSize,
      long batchDelayMs,
      boolean reprocessEnabled) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.reprocessEnabled = reprocessEnabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), getUpgradeIdUrn(), DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
      final Filter filter = reprocessEnabled ? null : missingLowercasedUrnFilter();
      if (reprocessEnabled) {
        log.info("{}: reprocess enabled, re-emitting aliases for every dataset.", id());
      }

      RunStats stats = new RunStats();
      String scrollId = null;

      do {
        ScrollResult result = scroll(filter, scrollId);
        if (result.getEntities().isEmpty()) {
          break;
        }
        emitPage(result.getEntities(), auditStamp, stats);
        scrollId = result.getScrollId();
        if (scrollId != null) {
          log.info("{}: emitted {} so far.", id(), stats.emitted);
          sleep();
        }
      } while (scrollId != null);

      context
          .upgrade()
          .setUpgradeResult(
              opContext,
              getUpgradeIdUrn(),
              entityService,
              DataHubUpgradeState.SUCCEEDED,
              Map.of(
                  "emitted", String.valueOf(stats.emitted),
                  "unparseable", String.valueOf(stats.unparseable)));
      log.info("{}: completed. {}", id(), stats);
      context.report().addLine(String.format("%s: completed. %s", id(), stats));

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * {@code keepAlive} is null on purpose: a scan of a large deployment outlives any point in time,
   * so pagination is {@code search_after} over the live index. The sort must then be {@code urn}
   * rather than the default {@code _score}, which is recomputed on write and would let a concurrent
   * ingestion move a document across the cursor and out of the scan entirely.
   */
  private ScrollResult scroll(@Nullable Filter filter, @Nullable String scrollId) {
    return searchService.scrollAcrossEntities(
        opContext.withSearchFlags(
            flags ->
                flags
                    .setFulltext(true)
                    .setSkipCache(true)
                    .setSkipHighlighting(true)
                    .setSkipAggregates(true)
                    // A dataset hidden from search today may be visible
                    // tomorrow, so every default that trims search results is turned off here.
                    .setIncludeSoftDeleted(true)
                    .setFilterNonLatestVersions(false)
                    .setIncludeHiddenLifecycleStages(true)),
        ImmutableList.of(DATASET_ENTITY_NAME),
        "*",
        filter,
        URN_SORT,
        scrollId,
        null,
        batchSize);
  }

  private void emitPage(Collection<SearchEntity> entities, AuditStamp auditStamp, RunStats stats) {
    List<MetadataChangeProposal> proposals = new ArrayList<>(entities.size());
    for (SearchEntity entity : entities) {
      buildProposal(entity.getEntity(), stats).ifPresent(proposals::add);
    }
    if (proposals.isEmpty()) {
      return;
    }

    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .mcps(proposals, auditStamp, opContext.getRetrieverContext())
            .build(opContext),
        true);
    stats.emitted += proposals.size();
  }

  private Optional<MetadataChangeProposal> buildProposal(Urn urn, RunStats stats) {
    final DatasetUrn lowercasedUrn;
    try {
      lowercasedUrn = AliasesUtils.lowercaseDatasetUrn(urn);
    } catch (URISyntaxException e) {
      // mirrors AliasesSideEffect: such urns can never carry the aspect
      log.warn("{}: skipping dataset urn {} that cannot be lowercased", id(), urn, e);
      stats.unparseable++;
      return Optional.empty();
    }

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(DATASET_ENTITY_NAME);
    proposal.setAspectName(ALIASES_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(backfillSystemMetadata());
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(new Aliases().setLowercasedUrn(lowercasedUrn)));
    return Optional.of(proposal);
  }

  private static Filter missingLowercasedUrnFilter() {
    CriterionArray criteria = new CriterionArray();
    criteria.add(buildIsNullCriterion(LOWERCASED_URN_FIELD));

    ConjunctiveCriterionArray conjunctiveCriteria = new ConjunctiveCriterionArray();
    conjunctiveCriteria.add(new ConjunctiveCriterion().setAnd(criteria));

    return new Filter().setOr(conjunctiveCriteria);
  }

  private SystemMetadata backfillSystemMetadata() {
    SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata(id());
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    systemMetadata.setProperties(properties);
    return systemMetadata;
  }

  private void sleep() {
    if (batchDelayMs > 0) {
      try {
        Thread.sleep(batchDelayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private static final class RunStats {
    private long emitted;
    private long unparseable;

    @Override
    public String toString() {
      return String.format("emitted=%d, unparseable=%d", emitted, unparseable);
    }
  }
}
