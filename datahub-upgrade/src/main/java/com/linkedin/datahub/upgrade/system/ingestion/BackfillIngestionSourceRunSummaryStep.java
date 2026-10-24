package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.ingestion.IngestionSourceRunSummaryUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Sets {@code dataHubIngestionSourceRunSummary} on existing ingestion sources from their most
 * recent execution request. Runs started after the upgrade keep it current through {@code
 * IngestionSourceRunSummarySideEffect}.
 */
@Slf4j
public class BackfillIngestionSourceRunSummaryStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillIngestionSourceRunSummaryStep_V1";
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
      Set<Urn> failedSources = new HashSet<>();
      String scrollId = null;
      do {
        scrollId = backfillBatch(auditStamp, scrollId, failedSources);
      } while (scrollId != null);

      // Without the marker the next system update retries; rewriting a summary is a no-op.
      if (!failedSources.isEmpty()) {
        log.error(
            "{} failed for {} ingestion source(s): {}", id(), failedSources.size(), failedSources);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
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
  private String backfillBatch(
      @Nonnull AuditStamp auditStamp, @Nullable String scrollId, @Nonnull Set<Urn> failedSources) {
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
      Urn sourceUrn = source.getEntity();
      try {
        Urn latestRun = latestExecutionRequest(sourceUrn);
        if (latestRun != null) {
          latestRunBySource.put(sourceUrn, latestRun);
        }
      } catch (RuntimeException e) {
        log.warn("Failed to find the latest execution request for {}", sourceUrn, e);
        failedSources.add(sourceUrn);
      }
    }

    if (!latestRunBySource.isEmpty()) {
      try {
        ingestSummaries(latestRunBySource, auditStamp);
      } catch (URISyntaxException | RuntimeException e) {
        log.warn("Failed to backfill run summaries for {}", latestRunBySource.keySet(), e);
        failedSources.addAll(latestRunBySource.keySet());
      }
    }
    return sources.getScrollId();
  }

  @Nullable
  private Urn latestExecutionRequest(@Nonnull Urn sourceUrn) {
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

  private void ingestSummaries(
      @Nonnull Map<Urn, Urn> latestRunBySource, @Nonnull AuditStamp auditStamp)
      throws URISyntaxException {
    Map<Urn, EntityResponse> runs =
        entityService.getEntitiesV2(
            opContext,
            EXECUTION_REQUEST_ENTITY_NAME,
            new HashSet<>(latestRunBySource.values()),
            Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME));
    Map<Urn, EntityResponse> sources =
        entityService.getEntitiesV2(
            opContext,
            INGESTION_SOURCE_ENTITY_NAME,
            latestRunBySource.keySet(),
            Set.of(INGESTION_INFO_ASPECT_NAME, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME));

    List<ChangeItemImpl> items =
        latestRunBySource.entrySet().stream()
            .map(
                entry ->
                    summaryUpsert(
                        entry.getKey(),
                        entry.getValue(),
                        runs.get(entry.getValue()),
                        sources.get(entry.getKey()),
                        auditStamp))
            .filter(Objects::nonNull)
            .toList();
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

  @Nullable
  private ChangeItemImpl summaryUpsert(
      @Nonnull Urn sourceUrn,
      @Nonnull Urn runUrn,
      @Nullable EntityResponse run,
      @Nullable EntityResponse source,
      @Nonnull AuditStamp auditStamp) {
    ExecutionRequestInput input =
        aspect(run, EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class);
    // Search can still return a source that was hard deleted; writing would recreate it.
    if (input == null
        || aspect(source, INGESTION_INFO_ASPECT_NAME, DataHubIngestionSourceInfo.class) == null) {
      return null;
    }
    DataHubIngestionSourceRunSummary summary =
        IngestionSourceRunSummaryUtils.buildSummary(
            runUrn,
            input,
            aspect(run, EXECUTION_REQUEST_RESULT_ASPECT_NAME, ExecutionRequestResult.class));
    return IngestionSourceRunSummaryUtils.shouldReplace(
            aspect(
                source,
                INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME,
                DataHubIngestionSourceRunSummary.class),
            summary)
        ? ChangeItemImpl.builder()
            .urn(sourceUrn)
            .changeType(ChangeType.UPSERT)
            .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
            .recordTemplate(summary)
            .auditStamp(auditStamp)
            .systemMetadata(createDefaultSystemMetadata())
            .build(opContext.getAspectRetriever())
        : null;
  }

  @Nullable
  private static <T extends RecordTemplate> T aspect(
      @Nullable EntityResponse response, @Nonnull String aspectName, @Nonnull Class<T> clazz) {
    EnvelopedAspect aspect = response != null ? response.getAspects().get(aspectName) : null;
    return aspect != null ? RecordUtils.toRecordTemplate(clazz, aspect.getValue().data()) : null;
  }
}
