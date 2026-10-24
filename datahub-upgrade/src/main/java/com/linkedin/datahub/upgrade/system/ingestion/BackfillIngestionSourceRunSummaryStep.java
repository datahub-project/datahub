package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.ingestion.sideeffects.IngestionSourceRunSummarySideEffect;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Sets {@code dataHubIngestionSourceRunSummary} on existing ingestion sources from their most
 * recent execution request. Runs started after the upgrade keep it current through {@link
 * IngestionSourceRunSummarySideEffect}.
 */
@Slf4j
public class BackfillIngestionSourceRunSummaryStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillIngestionSourceRunSummary";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  private static final String INGESTION_SOURCE_FIELD_NAME = "ingestionSource";
  private static final String REQUEST_TIME_MS_FIELD_NAME = "requestTimeMs";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final EntitySearchService entitySearchService;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public BackfillIngestionSourceRunSummaryStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      EntitySearchService entitySearchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.entitySearchService = entitySearchService;
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
      AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());
      String scrollId = null;
      do {
        scrollId = backfillBatch(auditStamp, scrollId);
      } while (scrollId != null);

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

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

  @Nullable
  private String backfillBatch(AuditStamp auditStamp, @Nullable String scrollId) {
    ScrollResult sources =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            List.of(INGESTION_SOURCE_ENTITY_NAME),
            "*",
            null,
            null,
            scrollId,
            null,
            batchSize);
    if (sources.getEntities().isEmpty()) {
      return null;
    }

    Map<Urn, Urn> latestRunBySource = new HashMap<>();
    for (SearchEntity source : sources.getEntities()) {
      Urn latestRun = latestExecutionRequest(source.getEntity());
      if (latestRun != null) {
        latestRunBySource.put(source.getEntity(), latestRun);
      }
    }

    if (!latestRunBySource.isEmpty()) {
      try {
        ingestSummaries(latestRunBySource, auditStamp);
      } catch (Exception e) {
        // A failed page leaves those sources without a summary until their next run.
        log.error("Failed to backfill ingestion source run summaries for a batch", e);
      }
    }
    return sources.getScrollId();
  }

  @Nullable
  private Urn latestExecutionRequest(Urn sourceUrn) {
    SearchResult runs =
        entitySearchService.filter(
            opContext,
            EXECUTION_REQUEST_ENTITY_NAME,
            QueryUtils.newFilter(INGESTION_SOURCE_FIELD_NAME, sourceUrn.toString()),
            List.of(
                new SortCriterion()
                    .setField(REQUEST_TIME_MS_FIELD_NAME)
                    .setOrder(SortOrder.DESCENDING)),
            0,
            1);
    return runs.getEntities().isEmpty() ? null : runs.getEntities().get(0).getEntity();
  }

  private void ingestSummaries(Map<Urn, Urn> latestRunBySource, AuditStamp auditStamp)
      throws URISyntaxException {
    Map<Urn, EntityResponse> runs =
        entityService.getEntitiesV2(
            opContext,
            EXECUTION_REQUEST_ENTITY_NAME,
            new HashSet<>(latestRunBySource.values()),
            Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME));
    Map<Urn, EntityResponse> existingSummaries =
        entityService.getEntitiesV2(
            opContext,
            INGESTION_SOURCE_ENTITY_NAME,
            latestRunBySource.keySet(),
            Set.of(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME));

    List<ChangeItemImpl> items = new ArrayList<>();
    latestRunBySource.forEach(
        (sourceUrn, runUrn) -> {
          EntityResponse run = runs.get(runUrn);
          EnvelopedAspect input =
              run != null ? run.getAspects().get(EXECUTION_REQUEST_INPUT_ASPECT_NAME) : null;
          if (input == null) {
            return;
          }
          EnvelopedAspect result = run.getAspects().get(EXECUTION_REQUEST_RESULT_ASPECT_NAME);
          DataHubIngestionSourceRunSummary summary =
              IngestionSourceRunSummarySideEffect.buildSummary(
                  runUrn,
                  new ExecutionRequestInput(input.getValue().data()),
                  result != null ? new ExecutionRequestResult(result.getValue().data()) : null);

          EntityResponse source = existingSummaries.get(sourceUrn);
          EnvelopedAspect existing =
              source != null
                  ? source.getAspects().get(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                  : null;
          if (!IngestionSourceRunSummarySideEffect.shouldReplace(
              existing != null
                  ? new DataHubIngestionSourceRunSummary(existing.getValue().data())
                  : null,
              summary)) {
            return;
          }
          items.add(
              ChangeItemImpl.builder()
                  .urn(sourceUrn)
                  .changeType(ChangeType.UPSERT)
                  .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
                  .recordTemplate(summary)
                  .auditStamp(auditStamp)
                  .systemMetadata(createDefaultSystemMetadata())
                  .build(opContext.getAspectRetriever()));
        });

    if (!items.isEmpty()) {
      entityService.ingestProposal(
          opContext,
          AspectsBatchImpl.builder()
              .retrieverContext(opContext.getRetrieverContext())
              .items(items)
              .build(opContext),
          false);
    }
  }
}
