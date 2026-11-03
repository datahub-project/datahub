package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

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
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.ingestion.IngestionSourceRunEvents;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Records {@code dataHubIngestionSourceRunEvent}s for existing ingestion sources: one for the
 * latest successful run, one for the latest failed run and one for the latest run. Runs started
 * after the upgrade are recorded by {@code IngestionSourceRunEventHook}.
 */
@Slf4j
public class BackfillIngestionSourceRunEventsStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillIngestionSourceRunEventsStep_V1";
  private static final String LAST_RUN_FIELD_NAME = "lastRunRequestedAtMillis";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  // Sources are written as the scroll walks them, so the cursor must not depend on score.
  private static final List<SortCriterion> SCROLL_SORT =
      List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING));

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final EntitySearchService entitySearchService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public BackfillIngestionSourceRunEventsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.entitySearchService = entitySearchService;
    this.timeseriesAspectService = timeseriesAspectService;
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

      // Without the marker the next system update retries; rewriting an event is a no-op.
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
    ScrollResult page =
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
            // Sources the hook has already recorded a run for are current. A forced reprocess
            // scrolls every source, but still skips a source whose recorded run is as new.
            reprocessEnabled
                ? null
                : QueryUtils.newFilter(CriterionUtils.buildNotExistsCriterion(LAST_RUN_FIELD_NAME)),
            SCROLL_SORT,
            scrollId,
            null,
            batchSize);
    if (page.getEntities().isEmpty()) {
      return null;
    }
    Set<Urn> sourceUrns =
        page.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toSet());
    try {
      Map<Urn, LatestIngestionRuns> runsBySource =
          entitySearchService.getLatestIngestionRuns(opContext, sourceUrns);
      List<MetadataChangeProposal> events = runEvents(runsBySource);
      if (!events.isEmpty()) {
        entityService.ingestProposal(
            opContext,
            AspectsBatchImpl.builder()
                .mcps(events, auditStamp, opContext.getRetrieverContext())
                .build(opContext),
            false);
      }
    } catch (RuntimeException e) {
      log.warn("Failed to backfill run events for ingestion sources {}", sourceUrns, e);
      failedSources.addAll(sourceUrns);
    }
    return page.getScrollId();
  }

  @Nonnull
  private List<MetadataChangeProposal> runEvents(
      @Nonnull Map<Urn, LatestIngestionRuns> runsBySource) {
    if (runsBySource.isEmpty()) {
      return List.of();
    }
    Set<Urn> runUrns =
        runsBySource.values().stream().flatMap(this::runUrns).collect(Collectors.toSet());
    Map<Urn, EntityResponse> runs =
        entityResponses(
            EXECUTION_REQUEST_ENTITY_NAME,
            runUrns,
            Set.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME, EXECUTION_REQUEST_RESULT_ASPECT_NAME));
    Map<Urn, EntityResponse> sources =
        entityResponses(
            INGESTION_SOURCE_ENTITY_NAME,
            runsBySource.keySet(),
            Set.of(INGESTION_INFO_ASPECT_NAME));
    Map<Urn, DataHubIngestionSourceRunEvent> recorded = recordedEvents(runsBySource.keySet());

    List<MetadataChangeProposal> proposals = new ArrayList<>();
    runsBySource.forEach(
        (sourceUrn, latestRuns) -> {
          // Search can still return a source that was deleted; writing would recreate it.
          EntityResponse source = sources.get(sourceUrn);
          if (source == null || !source.getAspects().containsKey(INGESTION_INFO_ASPECT_NAME)) {
            return;
          }
          // One event per source: the index keeps only the last event per source in a batch, so
          // the latest run's event carries the last success and failure times itself.
          DataHubIngestionSourceRunEvent event =
              runEvent(
                  latestRuns.getLatestRun(),
                  runs.get(latestRuns.getLatestRun()),
                  requestedAt(runs, latestRuns.getLatestSuccessfulRun()),
                  requestedAt(runs, latestRuns.getLatestFailedRun()));
          // The scroll filter is applied when the page is read, so the hook may have recorded a
          // run since. The source document keeps the last event written, so never write an
          // older one.
          DataHubIngestionSourceRunEvent current = recorded.get(sourceUrn);
          if (event != null
              && (current == null
                  || current.getRequestedAtMillis() < event.getRequestedAtMillis())) {
            proposals.add(
                AspectUtils.buildMetadataChangeProposal(
                    sourceUrn, INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, event));
          }
        });
    return proposals;
  }

  @Nonnull
  private Map<Urn, DataHubIngestionSourceRunEvent> recordedEvents(@Nonnull Set<Urn> sourceUrns) {
    Map<Urn, DataHubIngestionSourceRunEvent> recorded = new HashMap<>();
    timeseriesAspectService
        .getLatestTimeseriesAspectValues(
            opContext, sourceUrns, Set.of(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME), null)
        .forEach(
            (sourceUrn, aspects) -> {
              com.linkedin.metadata.aspect.EnvelopedAspect latest =
                  aspects.get(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME);
              if (latest != null) {
                GenericAspect value = latest.getAspect();
                recorded.put(
                    sourceUrn,
                    GenericRecordUtils.deserializeAspect(
                        value.getValue(),
                        value.getContentType(),
                        DataHubIngestionSourceRunEvent.class));
              }
            });
    return recorded;
  }

  @Nonnull
  private Stream<Urn> runUrns(@Nonnull LatestIngestionRuns latestRuns) {
    return Stream.of(
            latestRuns.getLatestRun(),
            latestRuns.getLatestSuccessfulRun(),
            latestRuns.getLatestFailedRun())
        .filter(Objects::nonNull);
  }

  @Nullable
  private static Long requestedAt(@Nonnull Map<Urn, EntityResponse> runs, @Nullable Urn runUrn) {
    ExecutionRequestInput input =
        runUrn != null
            ? aspect(
                runs.get(runUrn), EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class)
            : null;
    return input != null ? input.getRequestedAt() : null;
  }

  @Nullable
  private static DataHubIngestionSourceRunEvent runEvent(
      @Nonnull Urn runUrn,
      @Nullable EntityResponse run,
      @Nullable Long lastSuccessfulRequestedAtMillis,
      @Nullable Long lastFailedRequestedAtMillis) {
    ExecutionRequestInput input =
        aspect(run, EXECUTION_REQUEST_INPUT_ASPECT_NAME, ExecutionRequestInput.class);
    if (input == null) {
      return null;
    }
    ExecutionRequestResult result =
        aspect(run, EXECUTION_REQUEST_RESULT_ASPECT_NAME, ExecutionRequestResult.class);
    return IngestionSourceRunEvents.buildEvent(
        runUrn,
        input,
        result,
        IngestionSourceRunEvents.statusTimeMillis(input, result),
        lastSuccessfulRequestedAtMillis,
        lastFailedRequestedAtMillis);
  }

  @Nonnull
  private Map<Urn, EntityResponse> entityResponses(
      @Nonnull String entityName, @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    try {
      return entityService.getEntitiesV2(opContext, entityName, urns, aspectNames);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Invalid urn reading " + entityName + " " + urns, e);
    }
  }

  @Nullable
  private static <T extends RecordTemplate> T aspect(
      @Nullable EntityResponse response, @Nonnull String aspectName, @Nonnull Class<T> clazz) {
    EnvelopedAspect aspect = response != null ? response.getAspects().get(aspectName) : null;
    return aspect != null ? RecordUtils.toRecordTemplate(clazz, aspect.getValue().data()) : null;
  }
}
