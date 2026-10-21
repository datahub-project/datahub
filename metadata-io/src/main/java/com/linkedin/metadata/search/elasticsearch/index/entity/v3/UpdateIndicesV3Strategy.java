package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.CatchUpBatchRebuilder;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * V3 update indices strategy implementation for UpdateIndicesService. This handles the new v3
 * mapping approach with multi-entity indices.
 */
@Slf4j
public class UpdateIndicesV3Strategy implements UpdateIndicesStrategy {

  private final EntityIndexVersionConfiguration v3Config;
  private final ElasticSearchService elasticSearchService;
  private final V3SearchDocumentProjector searchDocumentProjector;
  private final TimeseriesAspectService timeseriesAspectService;
  private final MultiEntityMappingsBuilder mappingsBuilder;
  @Nullable private final TimeseriesWriteThrottleCache timeseriesThrottleCache;
  private static final Set<String> STRATEGY_OWNED_DOCUMENT_FIELDS =
      MappingConstants.STRATEGY_OWNED_ROOT_FIELDS;

  private final EntityDocumentIdHasher entityDocumentIdHasher;
  private final List<V3SearchDocumentContributor> documentContributors;
  private final boolean v2Enabled;
  @Nullable private final SemanticSearchConfiguration semanticSearchConfiguration;
  @Nullable private final IncrementalReindexWriteTargets incrementalReindexWriteTargets;
  private final CatchUpBatchRebuilder catchUpBatchRebuilder;

  /** Drains of a batch that mirrored deletes into a rebuild's index, before it is given up on. */
  private static final int MIRRORED_DELETE_DRAIN_ATTEMPTS = 3;

  /**
   * Built through {@link #builder()}. Unset optional parts take their defaults: the URN hasher, no
   * document contributors, V2 not dual-written, no semantic search configuration, no rollback
   * targets, no timeseries throttle.
   */
  @Builder
  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      @Nullable EntityDocumentIdHasher entityDocumentIdHasher,
      @Nullable List<V3SearchDocumentContributor> documentContributors,
      boolean v2Enabled,
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration,
      @Nullable IncrementalReindexWriteTargets incrementalReindexWriteTargets) {
    this.v3Config = v3Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentProjector = new V3SearchDocumentProjector(searchDocumentTransformer);
    this.timeseriesAspectService = timeseriesAspectService;
    this.timeseriesThrottleCache = timeseriesThrottleCache;
    this.entityDocumentIdHasher =
        entityDocumentIdHasher == null
            ? new Sha256UrnEntityDocumentIdHasher()
            : entityDocumentIdHasher;
    this.catchUpBatchRebuilder =
        new CatchUpBatchRebuilder(elasticSearchService, this.entityDocumentIdHasher);
    this.documentContributors =
        documentContributors == null ? List.of() : List.copyOf(documentContributors);
    this.v2Enabled = v2Enabled;
    this.semanticSearchConfiguration = semanticSearchConfiguration;
    this.incrementalReindexWriteTargets = incrementalReindexWriteTargets;
    try {
      this.mappingsBuilder =
          new MultiEntityMappingsBuilder(
              com.linkedin.metadata.config.search.EntityIndexConfiguration.builder()
                  .v3(v3Config)
                  .build());
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize V3 mappings builder", e);
    }
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (groupedEvents.isEmpty()) {
      return;
    }

    TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary =
        timeseriesThrottleCache != null ? timeseriesThrottleCache.newSummary() : null;

    log.debug("Processing {} URN groups with V3 unified batch optimization", groupedEvents.size());

    CatchUpBatchRebuilder.RebuiltBatches rebuilt =
        catchUpBatchRebuilder.rebuild(opContext, groupedEvents);
    List<Runnable> mirroredDeletes = new ArrayList<>();
    // Process each group of events for the same URN
    for (Map.Entry<Urn, List<MCLItem>> entry : rebuilt.batches().entrySet()) {
      Urn urn = entry.getKey();
      List<MCLItem> urnEvents = entry.getValue();

      log.debug("Processing {} events for URN: {} with V3 unified batch", urnEvents.size(), urn);

      // V3 optimization: single operation per URN regardless of aspect count
      mirroredDeletes.addAll(
          processUrnBatch(
              opContext,
              urn,
              urnEvents,
              rebuilt.removedAspects().getOrDefault(urn, Map.of()),
              rebuilt.indexedFields().getOrDefault(urn, Map.of()),
              structuredPropertiesHookEnabled,
              throttleSummary));
    }

    if (throttleSummary != null) {
      throttleSummary.logIfSuppressed();
    }

    // A backfill checkpoints what it has written once this returns, so its pages land before then.
    // A delete mirrored into the index a rebuild is filling must land too, and nothing replays a
    // hard delete, so such a batch is drained as well and a failed drain is retried. Any other
    // live batch is appended and committed, as before.
    boolean backfill =
        groupedEvents.values().stream()
            .flatMap(List::stream)
            .anyMatch(UpdateIndicesV3Strategy::isBackfillEvent);
    if (!mirroredDeletes.isEmpty()) {
      drainMirroredDeletes(mirroredDeletes);
    } else if (backfill) {
      elasticSearchService.getEsWriteDAO().flushAndWait();
    }
  }

  /**
   * Waits for a batch that mirrored deletes into an index a rebuild is filling to land. A drain
   * that reports failed writes has the mirrored deletes issued again and is repeated, a bounded
   * number of times; deleting a document that is already gone is a no-op, so the repeats are safe.
   * The last failure then propagates: the consumer records it and moves on, and the entity may
   * return when the fill swaps. A drain that merely runs out of time is not a failure: the bulk
   * processor is shared by every consumer thread and idles only when all of them pause, so the
   * mirrors are left queued with the processor's own retries behind them.
   */
  private void drainMirroredDeletes(@Nonnull List<Runnable> mirroredDeletes) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= MIRRORED_DELETE_DRAIN_ATTEMPTS; attempt++) {
      try {
        elasticSearchService.getEsWriteDAO().flushAndWait();
        return;
      } catch (RuntimeException e) {
        if (e.getCause() instanceof TimeoutException) {
          log.warn(
              "Draining a batch with {} deletes mirrored into a rebuild's index did not finish within"
                  + " the bulk transfer timeout; leaving them queued",
              mirroredDeletes.size());
          return;
        }
        last = e;
        log.warn(
            "Draining a batch with {} deletes mirrored into a rebuild's index failed (attempt {} of"
                + " {}); issuing the mirrored deletes again",
            mirroredDeletes.size(),
            attempt,
            MIRRORED_DELETE_DRAIN_ATTEMPTS,
            e);
        if (attempt < MIRRORED_DELETE_DRAIN_ATTEMPTS) {
          mirroredDeletes.forEach(Runnable::run);
        }
      }
    }
    throw last;
  }

  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    try {
      log.debug("Updating V3 index mappings for structured property change: {}", urn);

      if (Constants.STRUCTURED_PROPERTY_ENTITY_NAME.equals(entitySpec.getName())
          && Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspectSpec.getName())) {

        StructuredPropertyDefinition newDefinition =
            new StructuredPropertyDefinition(((RecordTemplate) newValue).data().copy());

        // Apply the mapping for the full set of currently-declared entity types on every
        // definition upsert, not just types newly added since oldValue. applyMappings is an
        // idempotent put_mapping, so re-saving a property re-applies (and thereby repairs) any
        // mapping a previous attempt failed to write — the only convergence path when the
        // structured-property system-update machinery is disabled (the default). Entity types
        // removed from the definition are handled by the dedicated removal path, not here.
        if (!newDefinition.getEntityTypes().isEmpty()) {

          // V3 uses the same approach as V2 but with V3 mappings and index convention
          log.info(
              "V3 structured property mapping update for {} - updating indices", newDefinition);

          elasticSearchService
              .buildReindexConfigsWithNewStructProp(opContext, urn, newDefinition)
              .forEach(
                  reindexState -> {
                    // Isolate failures per index: the property may declare multiple entity
                    // types, and one failing index must not prevent the mapping update from
                    // reaching the remaining declared entity types' indexes.
                    try {
                      log.info(
                          "Applying new V3 structured property {} to index {}",
                          newDefinition,
                          reindexState.name());
                      elasticSearchService
                          .getIndexBuilder(reindexState.name())
                          .applyMappings(opContext, reindexState, false);
                    } catch (Exception e) {
                      log.error(
                          "Failed to apply V3 structured property {} mapping to index {}",
                          urn,
                          reindexState.name(),
                          e);
                    }
                  });
        }
      }
    } catch (Exception e) {
      log.error("Issue with updating V3 index mappings for structured property change", e);
    }
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext) {
    return mappingsBuilder.getIndexMappings(opContext);
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return mappingsBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);
  }

  @Override
  public boolean isEnabled() {
    return v3Config.isEnabled();
  }

  @Override
  public boolean fillsNewIndicesFromPrimaryStorage() {
    return true;
  }

  /**
   * Processes all events for a single URN in V3 optimized fashion. Either deletes the entire entity
   * or performs a single upsert.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   */
  /** Returns the deletes the batch mirrored into an index a rebuild is filling, re-issuable. */
  @Nonnull
  private List<Runnable> processUrnBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nonnull Map<String, Set<String>> removedAspects,
      @Nonnull Map<String, Set<String>> indexedFields,
      boolean structuredPropertiesHookEnabled,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    try {
      return processUrnBatchUnchecked(
          opContext,
          urn,
          events,
          removedAspects,
          indexedFields,
          structuredPropertiesHookEnabled,
          throttleSummary);
    } catch (RuntimeException e) {
      // Live dual-write: one URN's V3 failure must not stall V2, and the entity's next event
      // rebuilds its document. A replay is different: nothing comes back for a URN it drops. A
      // baseline fill is the only pass over the historical corpus, and a catch-up replay is the
      // only pass over its window; the loader counts a URN as processed once this returns, and
      // the system-update catch-up step checkpoints after producing the event, so neither sends
      // it again. A replay's failure propagates instead: the loader retries the batch, splitting
      // it down to the single event, and otherwise fails the run with its checkpoint intact. A
      // removal's failure is skipped like any other live write's: the consumer records a failed
      // batch and moves on without redelivering it, so failing the batch would only drop the
      // other entities' writes with it. A mirror that could not be handed over is lost with the
      // skip, and the entity may return when the fill swaps. What can be retried, a delete that
      // was mirrored into an index a rebuild is filling, is retried when the batch is drained.
      final boolean replay = events.stream().anyMatch(UpdateIndicesV3Strategy::isBackfillEvent);
      if (v2Enabled && !replay) {
        log.error(
            "V3 search write failed for URN {} while V2 dual-write is enabled; skipping V3 document",
            urn,
            e);
        return List.of();
      }
      throw e;
    }
  }

  @Nonnull
  private List<Runnable> processUrnBatchUnchecked(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nonnull Map<String, Set<String>> removedAspects,
      @Nonnull Map<String, Set<String>> indexedFields,
      boolean structuredPropertiesHookEnabled,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    log.debug("V3 unified batch processing for URN: {} with {} events", urn, events.size());

    // Check if any event is a key aspect deletion - if so, delete the entire document
    boolean hasKeyAspectDeletion =
        events.stream()
            .anyMatch(
                event -> {
                  try {
                    // Only check for key aspect deletion if this is actually a DELETE event
                    if (event.getChangeType() == ChangeType.DELETE) {
                      Pair<EntitySpec, AspectSpec> specPair =
                          UpdateIndicesUtil.extractSpecPair(event);
                      return UpdateIndicesUtil.isDeletingKey(specPair);
                    }
                    return false;
                  } catch (Exception e) {
                    log.error(
                        "Error checking key aspect deletion for event {}: {}",
                        event.getAspectName(),
                        e.getMessage(),
                        e);
                    return false;
                  }
                });

    String docId = entityDocumentIdHasher.documentId(opContext, urn);
    String indexKey = v3IndexKey(events.get(0).getEntitySpec());
    // A baseline fill rebuilding into a next physical index names it on every event; the alias
    // keeps serving the old index until the fill swaps it.
    String fillTarget = fillTarget(events);
    if (hasKeyAspectDeletion) {
      if (fillTarget != null) {
        elasticSearchService.deleteDocumentByIndexName(opContext, fillTarget, docId);
      } else {
        elasticSearchService.deleteDocumentBySearchGroup(opContext, indexKey, docId);
      }
      log.debug(
          "V3 deleted entire document for URN: {} from index key: {} due to key aspect deletion",
          urn,
          indexKey);
      rollbackCopy(
          opContext,
          indexKey,
          oldBackingIndex ->
              elasticSearchService.deleteDocumentByIndexName(opContext, oldBackingIndex, docId));
      if (fillTarget == null) {
        return mirrorIntoFillTarget(
                opContext,
                indexKey,
                target -> elasticSearchService.deleteDocumentByIndexName(opContext, target, docId))
            .map(List::of)
            .orElse(List.of());
      }
      return List.of();
    }

    // Build the combined V3 document with _aspect structure
    ObjectNode combinedDocument =
        buildV3SearchDocument(
            opContext, urn, events, removedAspects, indexedFields, throttleSummary);

    // As on V2, the structured property mapping hook runs even when no aspect changed
    if (structuredPropertiesHookEnabled) {
      for (MCLItem event : events) {
        try {
          EntitySpec entitySpec = event.getEntitySpec();
          AspectSpec aspectSpec = event.getAspectSpec();
          updateIndexMappings(
              opContext,
              event.getUrn(),
              entitySpec,
              aspectSpec,
              event.getRecordTemplate(),
              event.getPreviousRecordTemplate());
        } catch (RuntimeException e) {
          log.error(
              "Error updating V3 index mappings for aspect {} of URN {}",
              event.getAspectName(),
              urn,
              e);
        }
      }
    }

    if (combinedDocument == null) {
      log.debug("V3 combined document is empty for URN: {}, skipping update", urn);
      return List.of();
    }

    // Write the combined document to Elasticsearch
    if (events.isEmpty()) {
      log.warn("V3 upsert attempted but no events available for URN: {}", urn);
      return List.of();
    }

    String finalDocument = combinedDocument.toString();

    if (fillTarget != null) {
      elasticSearchService.upsertDocumentByIndexName(opContext, fillTarget, finalDocument, docId);
    } else {
      elasticSearchService.upsertDocumentBySearchGroup(opContext, indexKey, finalDocument, docId);
    }
    log.debug(
        "V3 upserted combined document for URN: {} to index key: {} with {} aspects",
        urn,
        indexKey,
        events.size());

    // Append runIds to search document so rollback/list runs can find touched URNs (MAE path)
    List<String> distinctRunIds =
        events.stream()
            .filter(e -> e.getSystemMetadata() != null && e.getSystemMetadata().hasRunId())
            .map(e -> e.getSystemMetadata().getRunId())
            .distinct()
            .collect(Collectors.toList());
    for (String runId : distinctRunIds) {
      if (fillTarget != null) {
        elasticSearchService.appendRunIdByIndexName(opContext, fillTarget, docId, urn, runId);
      } else {
        elasticSearchService.appendRunIdBySearchGroup(opContext, indexKey, docId, urn, runId);
      }
    }

    // A removed aspect reaches the index a rebuild is filling the same way a removed entity does:
    // the fill may already have written the aspect, and nothing replays its removal.
    Optional<Runnable> mirrored =
        fillTarget == null && carriesDelete(events)
            ? mirrorIntoFillTarget(
                opContext,
                indexKey,
                target ->
                    elasticSearchService.upsertDocumentByIndexName(
                        opContext, target, finalDocument, docId))
            : Optional.empty();

    rollbackCopy(
        opContext,
        indexKey,
        oldBackingIndex -> {
          elasticSearchService.upsertDocumentByIndexName(
              opContext, oldBackingIndex, finalDocument, docId);
          for (String runId : distinctRunIds) {
            elasticSearchService.appendRunIdByIndexName(
                opContext, oldBackingIndex, docId, urn, runId);
          }
        });
    return mirrored.map(List::of).orElse(List.of());
  }

  /**
   * Repeats a write on the swapped-out backing index of {@code indexKey}'s alias while one awaits
   * rollback: same document, same hashed id, so the alias can be swapped back to a faithful copy.
   * The copy never fails the live write; a missed copy only degrades rollback fidelity.
   */
  private void rollbackCopy(
      @Nonnull OperationContext opContext,
      @Nonnull String indexKey,
      @Nonnull Consumer<String> write) {
    if (incrementalReindexWriteTargets == null) {
      return;
    }
    String alias =
        opContext.getSearchContext().getIndexConvention().getEntityIndexNameV3(opContext, indexKey);
    incrementalReindexWriteTargets
        .oldBackingIndexFor(alias)
        .ifPresent(
            oldBackingIndex -> {
              try {
                write.accept(oldBackingIndex);
                incrementalReindexWriteTargets.recordDualWriteStarted(alias);
              } catch (RuntimeException e) {
                log.error(
                    "Rollback dual-write to '{}' failed for index '{}'", oldBackingIndex, alias, e);
              }
            });
  }

  /**
   * Repeats a delete on the index a rebuild is filling for {@code indexKey}'s alias while that fill
   * is in flight: the alias still serves the old index, and catch-up re-derives its window from
   * primary storage, which holds nothing for a removed entity or aspect. Returns the mirrored
   * write, so the batch can issue it again if its drain fails, and lets a failure to hand it to the
   * bulk processor propagate: nothing replays a hard delete, so a mirror that does not land leaves
   * the entity in the next index across the swap.
   *
   * <p>Known limitation: a page the fill read before the delete and writes after this mirror puts
   * the entity, or the removed aspect's fields, back, since fill writes merge into the index and
   * nothing orders the two. The window is one page drain per entity; the next write to the entity,
   * or the orphan consistency check, repairs it.
   */
  @Nonnull
  private Optional<Runnable> mirrorIntoFillTarget(
      @Nonnull OperationContext opContext,
      @Nonnull String indexKey,
      @Nonnull Consumer<String> write) {
    if (incrementalReindexWriteTargets == null) {
      return Optional.empty();
    }
    String alias =
        opContext.getSearchContext().getIndexConvention().getEntityIndexNameV3(opContext, indexKey);
    return incrementalReindexWriteTargets
        .fillTargetFor(alias)
        .map(
            target -> {
              Runnable mirror = () -> write.accept(target);
              mirror.run();
              return mirror;
            });
  }

  /**
   * The physical index a baseline fill names on every event of {@code events}, or null when the
   * batch is not such a fill and the write goes to the alias.
   */
  @Nullable
  private static String fillTarget(@Nonnull List<MCLItem> events) {
    String target = null;
    for (MCLItem event : events) {
      if (!BackfillHeaders.MODE_BASELINE.equalsIgnoreCase(backfillMode(event))) {
        return null;
      }
      String named =
          event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.TARGET_INDEX_HEADER_NAME);
      if (named == null || named.isEmpty() || (target != null && !target.equals(named))) {
        return null;
      }
      target = named;
    }
    return target;
  }

  private static boolean isBackfillEvent(@Nonnull MCLItem event) {
    return backfillMode(event) != null;
  }

  private static boolean carriesDelete(@Nonnull List<MCLItem> events) {
    return events.stream().anyMatch(event -> event.getChangeType() == ChangeType.DELETE);
  }

  /** The backfill mode stamped on a replayed event, or null for a live write. */
  @Nullable
  private static String backfillMode(@Nonnull MCLItem event) {
    return event.getMetadataChangeLog() != null && event.getMetadataChangeLog().getHeaders() != null
        ? event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.MODE_HEADER_NAME)
        : null;
  }

  /**
   * Builds a V3 search document by combining multiple aspects into a single document with _aspect
   * structure.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   * @param removedAspects aspects a catch-up replay found in the document but not in storage, as
   *     aspect name to the field names the document holds for it; empty for a live write
   * @param indexedFields the raw per-URN field map the index reported for a catch-up replay; empty
   *     for a live write
   * @return the combined V3 document or null if no aspects to process
   */
  private ObjectNode buildV3SearchDocument(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nonnull Map<String, Set<String>> removedAspects,
      @Nonnull Map<String, Set<String>> indexedFields,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    EntitySpec entitySpec = events.get(0).getEntitySpec();
    String entityType = entitySpec.getName();
    ObjectNode combinedDocument = searchDocumentProjector.newEntityDocument(urn, entitySpec);

    boolean hasAnyAspects = false;
    List<BatchProjection> projections = new ArrayList<>();
    Set<String> changedRootFields = new HashSet<>();

    for (CoalescedAspectEvent coalesced : coalesceAspectEvents(events)) {
      MCLItem event = coalesced.event();
      try {
        if (isUnchanged(coalesced)) {
          opContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(this.getClass(), "search_diff_no_changes_detected", 1));
          // Unchanged, so there is nothing to diff against
          searchDocumentProjector
              .projectAspect(opContext, event, null)
              .ifPresent(unchanged -> projections.add(new BatchProjection(unchanged, true)));
          continue;
        }
        AspectSpec aspectSpec = event.getAspectSpec();
        String aspectName = aspectSpec.getName();

        // Throttle timeseries aspects for entity index writes
        if (aspectSpec.isTimeseries() && timeseriesThrottleCache != null) {
          long eventTimeMs =
              event.getAuditStamp() != null
                  ? event.getAuditStamp().getTime()
                  : System.currentTimeMillis();
          if (timeseriesThrottleCache.shouldThrottle(
              event.getEntitySpec().getName(), urn.toString(), aspectName, eventTimeMs)) {
            if (timeseriesThrottleCache.isEntityIndexEnabled()) {
              if (throttleSummary != null) {
                throttleSummary.recordSuppressed(
                    TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
              }
              continue;
            }
            if (timeseriesThrottleCache.isObserveEnabled() && throttleSummary != null) {
              throttleSummary.recordObserved();
            }
          }
        }

        // Each aspect's fields go to the document root and under _aspects.<aspect>; structured
        // properties go to the root only. In V3, timeseries aspects are treated the same as other
        // versioned aspects.
        Optional<V3SearchDocumentProjector.ProjectedAspect> projectedAspect =
            searchDocumentProjector.projectAspect(opContext, event, coalesced.baseline());
        if (projectedAspect.isPresent()) {
          projections.add(new BatchProjection(projectedAspect.get(), false));
          projectedAspect.get().rootFields().fieldNames().forEachRemaining(changedRootFields::add);
          hasAnyAspects = true;

          // recordWrite is handled by UpdateIndicesService after all strategies have processed
          if (aspectSpec.isTimeseries() && throttleSummary != null) {
            throttleSummary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
          }

          if (aspectSpec.isTimeseries()) {
            log.debug(
                "V3 included timeseries aspect {} in combined document for URN: {}",
                aspectName,
                urn);
          } else {
            log.debug(
                "V3 included versioned aspect {} in combined document for URN: {}",
                aspectName,
                urn);
          }
        }

      } catch (Exception e) {
        log.error(
            "Error processing aspect {} for URN {}: {}",
            event.getAspectName(),
            urn,
            e.getMessage(),
            e);
      }
    }

    if (!hasAnyAspects && removedAspects.isEmpty()) {
      return null;
    }

    // Aspects the batch left unchanged are not rewritten, but they keep their place in the order
    // for the root fields a changed aspect writes, so a shared root field ends with the value of
    // the last aspect in the batch that sets it
    for (BatchProjection projection : projections) {
      searchDocumentProjector.applyProjection(
          combinedDocument,
          projection.unchanged()
              ? sharedRootValues(projection.projected(), changedRootFields)
              : keepBatchRootValues(combinedDocument, projection.projected()));
    }

    Set<String> timeseriesRootFields = timeseriesRootFields(entitySpec, indexedFields);
    removeAspects(combinedDocument, removedAspects, timeseriesRootFields);
    liftSemanticEmbeddingsToRoot(opContext, urn, entityType, events, combinedDocument);

    applyDocumentContributors(opContext, urn, combinedDocument);
    if (isCatchUpBatch(events)) {
      clearStaleCatchUpFields(combinedDocument, events, indexedFields, timeseriesRootFields);
    }
    return combinedDocument;
  }

  /**
   * Nulls what the document holds for aspects primary storage no longer has, as a catch-up replay
   * found them in the index: the entry under {@code _aspects} and the root fields the aspect
   * projected, except a root field an aspect of this batch still sets, which keeps its value, a
   * root field a timeseries aspect in the index still holds, and the fields the strategy itself
   * owns. Structured properties are dotted root keys, so losing that aspect nulls those keys and
   * leaves the parent key alone. The merge the document is written with then clears them.
   */
  private static void removeAspects(
      @Nonnull ObjectNode document,
      @Nonnull Map<String, Set<String>> removedAspects,
      @Nonnull Set<String> timeseriesRootFields) {
    if (removedAspects.isEmpty()) {
      return;
    }
    JsonNode existing = document.get(MappingConstants.ASPECTS_FIELD_NAME);
    ObjectNode aspects =
        existing instanceof ObjectNode node
            ? node
            : document.putObject(MappingConstants.ASPECTS_FIELD_NAME);
    removedAspects.forEach(
        (aspectName, fields) -> {
          if (Constants.STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
            nullMissingStructuredPropertyKeys(document, fields);
            return;
          }
          aspects.putNull(aspectName);
          for (String field : fields) {
            if (rootFieldKept(document, field, timeseriesRootFields)) {
              continue;
            }
            nullIfAbsent(document, field);
          }
        });
  }

  /**
   * After a catch-up document is fully built, nulls index fields the restate no longer sets. A
   * child missing from a restated aspect is nulled under {@code _aspects} and, when the finished
   * document has no value there, at the root. A key missing from an object field the document still
   * has, such as {@code customProperties}, is nulled inside that object. A root field a timeseries
   * aspect in the index still holds, or that the strategy owns, stays. Structured properties are
   * dotted root keys the finished document does not set; the parent key is left alone. A leftover
   * {@code _aspects.structuredProperties} entry is nulled without reading its children as those
   * keys.
   */
  private static void clearStaleCatchUpFields(
      @Nonnull ObjectNode document,
      @Nonnull List<MCLItem> events,
      @Nonnull Map<String, Set<String>> indexedFields,
      @Nonnull Set<String> timeseriesRootFields) {
    if (indexedFields.isEmpty()) {
      return;
    }
    nullMissingStructuredPropertyKeys(
        document,
        indexedFields.getOrDefault(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME, Set.of()));
    if (indexedFields.containsKey(ESSearchDAO.STALE_ASPECTS_STRUCTURED_PROPERTIES)) {
      aspectsObject(document).putNull(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    }

    Set<String> restated = new HashSet<>();
    for (MCLItem event : events) {
      if (event.getChangeType() == ChangeType.RESTATE && event.getRecordTemplate() != null) {
        restated.add(event.getAspectName());
      }
    }
    JsonNode aspectsNode = document.get(MappingConstants.ASPECTS_FIELD_NAME);
    ObjectNode aspects = aspectsNode instanceof ObjectNode node ? node : null;
    for (String aspectName : restated) {
      if (Constants.STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
        continue;
      }
      Set<String> fields = indexedFields.get(aspectName);
      if (fields == null) {
        continue;
      }
      JsonNode aspectNode = aspects == null ? null : aspects.get(aspectName);
      ObjectNode aspect = aspectNode instanceof ObjectNode node ? node : null;
      for (String field : fields) {
        if (aspect != null && !STRATEGY_OWNED_DOCUMENT_FIELDS.contains(field)) {
          nullIfAbsent(aspect, field);
        }
        if (rootFieldKept(document, field, timeseriesRootFields)) {
          continue;
        }
        nullIfAbsent(document, field);
      }
    }
  }

  /** Dotted {@code structuredProperties.<name>} keys the document does not already set. */
  private static void nullMissingStructuredPropertyKeys(
      @Nonnull ObjectNode document, @Nonnull Set<String> fields) {
    for (String field : fields) {
      if (isStructuredPropertyKey(field) && !document.hasNonNull(field)) {
        document.putNull(field);
      }
    }
  }

  private static boolean isStructuredPropertyKey(@Nonnull String field) {
    return field.startsWith(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME + ".");
  }

  /**
   * A root field the catch-up document already sets, that a timeseries aspect in the index holds,
   * or that the strategy owns, is not nulled.
   */
  private static boolean rootFieldKept(
      @Nonnull ObjectNode document,
      @Nonnull String field,
      @Nonnull Set<String> timeseriesRootFields) {
    return STRATEGY_OWNED_DOCUMENT_FIELDS.contains(field)
        || V3SearchDocumentProjector.SYSTEM_METADATA_FIELD.equals(field)
        || timeseriesRootFields.contains(field)
        || valuePresent(document, field);
  }

  /**
   * Nulls {@code path} where the finished document does not already have it. A path with {@link
   * ESSearchDAO#NESTED_FIELD_SEPARATOR} is a key inside an object field; the child is nulled only
   * when that object is present, so a missing parent is cleared by its own top-level name rather
   * than by inventing the object here.
   */
  private static void nullIfAbsent(@Nonnull ObjectNode root, @Nonnull String path) {
    String[] parts = path.split(ESSearchDAO.NESTED_FIELD_SEPARATOR, -1);
    ObjectNode node = root;
    for (int i = 0; i < parts.length - 1; i++) {
      JsonNode child = node.get(parts[i]);
      if (!(child instanceof ObjectNode object)) {
        return;
      }
      node = object;
    }
    if (!node.has(parts[parts.length - 1])) {
      node.putNull(parts[parts.length - 1]);
    }
  }

  private static boolean valuePresent(@Nonnull ObjectNode root, @Nonnull String path) {
    String[] parts = path.split(ESSearchDAO.NESTED_FIELD_SEPARATOR, -1);
    JsonNode node = root;
    for (String part : parts) {
      if (!(node instanceof ObjectNode object) || !object.has(part) || object.get(part).isNull()) {
        return false;
      }
      node = object.get(part);
    }
    return true;
  }

  /**
   * Root fields the timeseries aspects the index reports still hold. A catch-up replay rebuilds
   * versioned aspects only, so neither a removed aspect nor a restate that no longer sets one of
   * these fields may null it.
   */
  @Nonnull
  private static Set<String> timeseriesRootFields(
      @Nonnull EntitySpec entitySpec, @Nonnull Map<String, Set<String>> indexedFields) {
    Set<String> fields = new HashSet<>();
    indexedFields.forEach(
        (aspectName, aspectFields) -> {
          if (isTimeseriesAspect(entitySpec, aspectName)) {
            fields.addAll(aspectFields);
          }
        });
    return fields;
  }

  private static boolean isTimeseriesAspect(
      @Nonnull EntitySpec entitySpec, @Nonnull String aspectName) {
    return Boolean.TRUE.equals(entitySpec.hasAspect(aspectName))
        && entitySpec.getAspectSpec(aspectName).isTimeseries();
  }

  private static boolean isCatchUpBatch(@Nonnull List<MCLItem> events) {
    return events.stream()
        .anyMatch(event -> BackfillHeaders.MODE_CATCH_UP.equalsIgnoreCase(backfillMode(event)));
  }

  @Nonnull
  private static ObjectNode aspectsObject(@Nonnull ObjectNode document) {
    JsonNode existing = document.get(MappingConstants.ASPECTS_FIELD_NAME);
    return existing instanceof ObjectNode node
        ? node
        : document.putObject(MappingConstants.ASPECTS_FIELD_NAME);
  }

  /**
   * The event that writes an aspect, the aspect value to diff it against, and whether the aspect
   * must be written even when that value equals the last one.
   */
  private record CoalescedAspectEvent(
      @Nonnull MCLItem event, @Nullable RecordTemplate baseline, boolean mustWrite) {}

  private static boolean isForceIndexing(@Nonnull MCLItem event) {
    SystemMetadata systemMetadata = event.getSystemMetadata();
    return systemMetadata != null
        && systemMetadata.getProperties() != null
        && Boolean.parseBoolean(systemMetadata.getProperties().get(Constants.FORCE_INDEXING_KEY));
  }

  /** An aspect's projection, and whether the batch left the aspect unchanged. */
  private record BatchProjection(
      @Nonnull V3SearchDocumentProjector.ProjectedAspect projected, boolean unchanged) {}

  /**
   * One event per aspect, in order of first appearance: the last event in the batch, diffed against
   * the first previous value of the batch, as V2 does when it coalesces a batch. That value is what
   * the index held before the batch, or, when the batch restated the aspect, the value it held
   * right after that, so removal nulls cover every field the batch dropped; diffing each event
   * against its own predecessor would lose the nulls of all but the last event under {@code
   * _aspects.<aspect>}, which each event replaces. An aspect the batch created has no baseline: the
   * index held none of its values, and a null for one could only clear a root field another aspect
   * supplies. An aspect the batch created or forced is always written.
   *
   * <p>Two aspects can project the same root field (e.g. corpuser displayName from CorpUserInfo and
   * CorpUserEditableInfo). Root fields are last-write-wins, so user-edited override aspects are
   * applied after their ingested base aspect.
   */
  @Nonnull
  private static List<CoalescedAspectEvent> coalesceAspectEvents(@Nonnull List<MCLItem> events) {
    Map<String, MCLItem> lastEventByAspect = new LinkedHashMap<>();
    Map<String, RecordTemplate> baselineByAspect = new HashMap<>();
    // Aspects the batch must write even if their value looks unchanged: the index may not hold the
    // first event's value (it had no previous value), or an event forces indexing
    Set<String> mustWrite = new HashSet<>();
    Set<String> created = new HashSet<>();
    for (MCLItem event : events) {
      String aspectName = event.getAspectName();
      if (!lastEventByAspect.containsKey(aspectName) && event.getPreviousRecordTemplate() == null) {
        mustWrite.add(aspectName);
        if (event.getChangeType() != ChangeType.RESTATE) {
          created.add(aspectName);
        }
      }
      if (isForceIndexing(event)) {
        mustWrite.add(aspectName);
      }
      // A restate carries no previous value, so a later event's previous value is the baseline
      if (baselineByAspect.get(aspectName) == null && !created.contains(aspectName)) {
        baselineByAspect.put(aspectName, event.getPreviousRecordTemplate());
      }
      lastEventByAspect.put(aspectName, event);
    }
    return lastEventByAspect.entrySet().stream()
        .sorted(Comparator.comparing(entry -> isEditableOverrideAspect(entry.getKey())))
        .map(
            entry ->
                new CoalescedAspectEvent(
                    entry.getValue(),
                    baselineByAspect.get(entry.getKey()),
                    mustWrite.contains(entry.getKey())))
        .collect(Collectors.toList());
  }

  /**
   * Skips an aspect whose value did not change, as V2 does outside forced indexing. Rewriting it
   * would let an unchanged base aspect overwrite the root value of its user-edited override.
   */
  private static boolean isUnchanged(@Nonnull CoalescedAspectEvent coalesced) {
    MCLItem event = coalesced.event();
    return !coalesced.mustWrite()
        && event.getChangeType() != ChangeType.DELETE
        && coalesced.baseline() != null
        && event.getRecordTemplate() != null
        && coalesced.baseline().data() != null
        && coalesced.baseline().data().equals(event.getRecordTemplate().data());
  }

  /**
   * A null root value means the aspect dropped the field. When an aspect applied earlier in the
   * batch still sets it, as when a user-edited displayName is removed while the ingested one stays,
   * the earlier value is kept; the null still clears the field under {@code _aspects.<aspect>}.
   */
  @Nonnull
  private static V3SearchDocumentProjector.ProjectedAspect keepBatchRootValues(
      @Nonnull ObjectNode document, @Nonnull V3SearchDocumentProjector.ProjectedAspect projected) {
    ObjectNode rootFields = projected.rootFields().deepCopy();
    List<String> setEarlier = new ArrayList<>();
    rootFields
        .fieldNames()
        .forEachRemaining(
            name -> {
              if (rootFields.get(name).isNull() && document.hasNonNull(name)) {
                setEarlier.add(name);
              }
            });
    rootFields.remove(setEarlier);
    return new V3SearchDocumentProjector.ProjectedAspect(
        projected.aspectName(), rootFields, projected.aspectFields(), projected.rootOnly());
  }

  /**
   * The root values of an unchanged aspect, limited to the root fields a changed aspect of the
   * batch writes. Its other root fields and its {@code _aspects.<aspect>} entry are not rewritten,
   * so an unchanged aspect cannot overwrite a value another aspect set in an earlier batch.
   */
  @Nonnull
  private static V3SearchDocumentProjector.ProjectedAspect sharedRootValues(
      @Nonnull V3SearchDocumentProjector.ProjectedAspect unchanged,
      @Nonnull Set<String> changedRootFields) {
    ObjectNode rootFields = JsonNodeFactory.instance.objectNode();
    unchanged
        .rootFields()
        .fieldNames()
        .forEachRemaining(
            name -> {
              if (changedRootFields.contains(name) && unchanged.rootFields().hasNonNull(name)) {
                rootFields.set(name, unchanged.rootFields().get(name).deepCopy());
              }
            });
    return new V3SearchDocumentProjector.ProjectedAspect(
        unchanged.aspectName(), rootFields, JsonNodeFactory.instance.objectNode(), true);
  }

  // DataHub names user-override aspects with "editable"/"Editable" (corpUserEditableInfo,
  // corpGroupEditableInfo, editableDatasetProperties, editableSchemaMetadata, ...). These overrides
  // conventionally take precedence over their ingested base aspect, so they are applied last when
  // resolving a shared root convenience field.
  private static boolean isEditableOverrideAspect(@Nullable final String aspectName) {
    return aspectName != null && aspectName.toLowerCase(Locale.ROOT).contains("editable");
  }

  private void liftSemanticEmbeddingsToRoot(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityType,
      @Nonnull List<MCLItem> events,
      @Nonnull ObjectNode combinedDocument) {
    if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticSearchConfiguration, entityType)) {
      return;
    }
    JsonNode aspects = combinedDocument.get(MappingConstants.ASPECTS_FIELD_NAME);
    if (aspects != null && aspects.isObject() && aspects.has("semanticContent")) {
      JsonNode semanticContentNode = aspects.get("semanticContent");
      if (semanticContentNode instanceof ObjectNode semanticContent) {
        for (String field :
            List.of(
                SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
                SemanticEmbeddingMappings.SKIP_REASON_FIELD,
                SemanticEmbeddingMappings.SKIPPED_AT_FIELD)) {
          if (semanticContent.has(field)) {
            combinedDocument.set(field, semanticContent.get(field));
            semanticContent.remove(field);
          }
        }
      }
    }
    boolean shouldStamp =
        events.stream()
            .map(MCLItem::getAspectName)
            .anyMatch(SemanticDocumentProvenance::isStampEligibleAspect);
    if (!shouldStamp) {
      return;
    }
    // Combined V3 docs keep embed text under _aspects, so stamp as semanticContent (fetch both
    // sides) regardless of which content aspect arrived first in the batch.
    SemanticDocumentProvenance.stampResolvedTextSha256(
        opContext, urn, entityType, "semanticContent", combinedDocument);
  }

  private void applyDocumentContributors(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull ObjectNode document) {
    for (V3SearchDocumentContributor contributor : documentContributors) {
      ObjectNode extras = JsonNodeFactory.instance.objectNode();
      contributor.contribute(opContext, urn, extras);
      extras
          .fields()
          .forEachRemaining(
              entry -> {
                String fieldName = entry.getKey();
                if (STRATEGY_OWNED_DOCUMENT_FIELDS.contains(fieldName) || document.has(fieldName)) {
                  throw new IllegalStateException(
                      "V3 search document contributor attempted to overwrite field '"
                          + fieldName
                          + "' for "
                          + urn);
                }
                document.set(fieldName, entry.getValue());
              });
    }
  }

  @Nonnull
  private String v3IndexKey(@Nonnull EntitySpec entitySpec) {
    return V3IndexKeys.resolve(entitySpec);
  }
}
