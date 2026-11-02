package com.linkedin.metadata.service;

import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.PrimaryStorageContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Replaces a catch-up replay's batches with the entity's stored state.
 *
 * <p>A catch-up replay describes rows read from a snapshot of primary storage. Each replayed URN's
 * batch is replaced by the entity's stored aspects as they stand when it is processed, so a value
 * that changed since the read is not overlaid with the stale row, and an entity that is gone is
 * deleted whatever the replayed row says: one RESTATE per stored aspect, or a DELETE of the key
 * aspect when the entity has none, each carrying the replayed event's headers so the writer still
 * treats the batch as a replay. The read goes to primary storage itself: a lagging read replica
 * could hand back the very state the replay is meant to correct. Batches without the catch-up mark
 * are returned as they are.
 *
 * <p>The rebuilt events can only write what storage holds. What the index holds for an aspect
 * storage no longer has, after a removal nothing mirrored, would survive the writer's merge, so the
 * rebuilder also reads, with one request per slice, the aspect names each replayed document carries
 * and reports the versioned ones storage lost, with the field names the document holds for them,
 * for the writer to null. Timeseries entries are left alone: storage never lists them.
 *
 * <p>Two consequences are deliberate. An entity whose key aspect is absent from storage is treated
 * as gone even if other aspect rows remain, so such a legacy entity is removed from the index on
 * replay. And a storage or index read that fails propagates out of the whole batch: a replay is the
 * only pass over its window, so the failure must surface rather than drop the URN; the loader
 * retries the batch, and the consumer records the failure.
 */
@Slf4j
public final class CatchUpBatchRebuilder {

  /** URNs per storage read and per index read; bounds each query and the entities in memory. */
  private static final int READ_SLICE_SIZE = 500;

  /**
   * The batches to process in place of a replay's; per entity, the versioned aspects its document
   * holds that storage no longer has; and the raw per-URN field map the index reported, including
   * aspects storage still holds, timeseries aspects, dotted structured-property keys, and a stale
   * {@code _aspects.structuredProperties} marker.
   */
  public record RebuiltBatches(
      @Nonnull Map<Urn, List<MCLItem>> batches,
      @Nonnull Map<Urn, Map<String, Set<String>>> removedAspects,
      @Nonnull Map<Urn, Map<String, Set<String>>> indexedFields) {}

  private final ElasticSearchService elasticSearchService;
  private final EntityDocumentIdHasher documentIdHasher;

  public CatchUpBatchRebuilder(
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull EntityDocumentIdHasher documentIdHasher) {
    this.elasticSearchService = elasticSearchService;
    this.documentIdHasher = documentIdHasher;
  }

  /** The batches to process in place of {@code groupedEvents}; a batch without the mark is kept. */
  @Nonnull
  public RebuiltBatches rebuild(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, List<MCLItem>> groupedEvents) {
    Map<Urn, Set<String>> storedAspectNames = new HashMap<>();
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      if (entry.getValue().stream().anyMatch(CatchUpBatchRebuilder::isCatchUpEvent)) {
        storedAspectNames.put(
            entry.getKey(), storedAspectNames(entry.getValue().get(0).getEntitySpec()));
      }
    }
    if (storedAspectNames.isEmpty()) {
      return new RebuiltBatches(groupedEvents, Map.of(), Map.of());
    }
    OperationContext primary = primaryStorageContext(opContext);
    List<Map<Urn, Set<String>>> slices = reads(storedAspectNames);
    Map<Urn, Map<String, SystemAspect>> stored = new HashMap<>();
    for (Map<Urn, Set<String>> slice : slices) {
      stored.putAll(
          Optional.ofNullable(primary.getAspectRetriever().getLatestSystemAspects(primary, slice))
              .orElse(Map.of()));
    }
    Map<Urn, Map<String, Set<String>>> removedAspects = new HashMap<>();
    Map<Urn, Map<String, Set<String>>> indexedFields = new HashMap<>();
    for (Map<Urn, Set<String>> slice : slices) {
      IndexFields read = indexFields(opContext, slice, stored);
      removedAspects.putAll(read.removed());
      indexedFields.putAll(read.indexed());
    }
    Map<Urn, List<MCLItem>> rebuilt = new LinkedHashMap<>(groupedEvents);
    for (Urn urn : storedAspectNames.keySet()) {
      rebuilt.put(
          urn,
          storedStateEvents(
              opContext, urn, groupedEvents.get(urn), stored.getOrDefault(urn, Map.of())));
    }
    return new RebuiltBatches(rebuilt, removedAspects, indexedFields);
  }

  /** One slice's index read: aspects storage has lost, and the raw field map for every hit. */
  private record IndexFields(
      @Nonnull Map<Urn, Map<String, Set<String>>> removed,
      @Nonnull Map<Urn, Map<String, Set<String>>> indexed) {}

  /**
   * Per entity of one slice, the versioned aspects its document holds that storage no longer has,
   * each with the field names the document holds for it, and the raw map the index returned; one
   * index request per slice. An entity with no key aspect in storage is deleted whole, so it
   * contributes nothing to remove, and an entity the index does not hold has nothing to remove. The
   * raw map is still returned for every hit, including timeseries aspects storage never lists.
   */
  @Nonnull
  private IndexFields indexFields(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, Set<String>> slice,
      @Nonnull Map<Urn, Map<String, SystemAspect>> stored) {
    EntitySpec entitySpec =
        opContext
            .getEntityRegistry()
            .getEntitySpec(slice.keySet().iterator().next().getEntityType());
    Set<String> versioned = storedAspectNames(entitySpec);
    Map<String, Urn> urnsByDocumentId = new LinkedHashMap<>();
    for (Urn urn : slice.keySet()) {
      urnsByDocumentId.put(documentIdHasher.documentId(opContext, urn), urn);
    }
    Map<String, Map<String, Set<String>>> inIndex =
        elasticSearchService.aspectFieldNamesBySearchGroup(
            opContext, V3IndexKeys.resolve(entitySpec), urnsByDocumentId.keySet());
    Map<Urn, Map<String, Set<String>>> removed = new HashMap<>();
    Map<Urn, Map<String, Set<String>>> indexed = new HashMap<>();
    inIndex.forEach(
        (documentId, aspectsInIndex) -> {
          Urn urn = urnsByDocumentId.get(documentId);
          if (urn == null) {
            return;
          }
          indexed.put(urn, aspectsInIndex);
          Map<String, SystemAspect> storedAspects = stored.getOrDefault(urn, Map.of());
          if (!isStored(storedAspects, entitySpec.getKeyAspectName())) {
            return;
          }
          Map<String, Set<String>> gone = new HashMap<>();
          aspectsInIndex.forEach(
              (aspectName, fields) -> {
                if (versioned.contains(aspectName) && !isStored(storedAspects, aspectName)) {
                  gone.put(aspectName, fields);
                }
              });
          if (!gone.isEmpty()) {
            log.info("Catch-up replay drops {} from the document of {}", gone.keySet(), urn);
            removed.put(urn, gone);
          }
        });
    return new IndexFields(removed, indexed);
  }

  private static boolean isStored(
      @Nonnull Map<String, SystemAspect> storedAspects, @Nonnull String aspectName) {
    SystemAspect stored = storedAspects.get(aspectName);
    return stored != null && stored.getRecordTemplate() != null;
  }

  /**
   * The storage reads for one batch. The retriever resolves one entity type per call, from the
   * first URN it is given, so each read holds URNs of a single type; and a replay page (the
   * operator catch-up hands over pages of thousands of rows) is read in bounded slices so no single
   * query carries every entity of the page.
   */
  @Nonnull
  private static List<Map<Urn, Set<String>>> reads(@Nonnull Map<Urn, Set<String>> requested) {
    Map<String, Map<Urn, Set<String>>> byEntityType = new LinkedHashMap<>();
    requested.forEach(
        (urn, aspectNames) ->
            byEntityType
                .computeIfAbsent(urn.getEntityType(), ignored -> new LinkedHashMap<>())
                .put(urn, aspectNames));
    List<Map<Urn, Set<String>>> reads = new ArrayList<>();
    for (Map<Urn, Set<String>> ofOneType : byEntityType.values()) {
      Map<Urn, Set<String>> slice = new LinkedHashMap<>();
      for (Map.Entry<Urn, Set<String>> entry : ofOneType.entrySet()) {
        slice.put(entry.getKey(), entry.getValue());
        if (slice.size() == READ_SLICE_SIZE) {
          reads.add(slice);
          slice = new LinkedHashMap<>();
        }
      }
      if (!slice.isEmpty()) {
        reads.add(slice);
      }
    }
    return reads;
  }

  private static Set<String> storedAspectNames(@Nonnull EntitySpec entitySpec) {
    return entitySpec.getAspectSpecs().stream()
        .filter(aspectSpec -> !aspectSpec.isTimeseries())
        .map(AspectSpec::getName)
        .collect(Collectors.toSet());
  }

  /**
   * The events a catch-up batch is processed as: one RESTATE per stored aspect, or a DELETE of the
   * key aspect when the entity is gone, each carrying the replayed event's headers.
   */
  @Nonnull
  private static List<MCLItem> storedStateEvents(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> replayed,
      @Nonnull Map<String, SystemAspect> storedAspects) {
    // A batch can carry a live event next to the replayed one; the mark, and the headers that go
    // with it, come from a replayed event.
    MCLItem replayedEvent =
        replayed.stream().filter(CatchUpBatchRebuilder::isCatchUpEvent).findFirst().orElseThrow();
    EntitySpec entitySpec = replayedEvent.getEntitySpec();
    StringMap headers = replayedEvent.getMetadataChangeLog().getHeaders();
    AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    if (!isStored(storedAspects, entitySpec.getKeyAspectName())) {
      log.info(
          "Catch-up replay finds no key aspect in primary storage for {}; deleting its document",
          urn);
      MetadataChangeLog delete =
          constructMCL(
              null,
              entitySpec.getName(),
              urn,
              ChangeType.DELETE,
              entitySpec.getKeyAspectName(),
              replayedEvent.getAuditStamp(),
              null,
              replayedEvent.getSystemMetadata(),
              null,
              null);
      delete.setHeaders(headers);
      return List.of(MCLItemImpl.builder().build(delete, aspectRetriever));
    }
    return storedAspects.entrySet().stream()
        .filter(entry -> isStored(storedAspects, entry.getKey()))
        .sorted(Map.Entry.comparingByKey())
        .map(
            entry -> {
              SystemAspect stored = entry.getValue();
              MetadataChangeLog restate =
                  constructMCL(
                      null,
                      entitySpec.getName(),
                      urn,
                      ChangeType.RESTATE,
                      entry.getKey(),
                      stored.getAuditStamp(),
                      stored.getRecordTemplate(),
                      stored.getSystemMetadata(),
                      null,
                      null);
              restate.setHeaders(headers);
              return (MCLItem) MCLItemImpl.builder().build(restate, aspectRetriever);
            })
        .collect(Collectors.toList());
  }

  private static OperationContext primaryStorageContext(@Nonnull OperationContext opContext) {
    PrimaryStorageContext storage = opContext.getPrimaryStorageContext();
    if (storage == null || storage.getReadPreference() == ReadPreference.PRIMARY) {
      return opContext;
    }
    return OperationContext.withReadPreference(opContext, ReadPreference.PRIMARY);
  }

  private static boolean isCatchUpEvent(@Nonnull MCLItem event) {
    MetadataChangeLog changeLog = event.getMetadataChangeLog();
    return changeLog != null
        && changeLog.getHeaders() != null
        && BackfillHeaders.MODE_CATCH_UP.equalsIgnoreCase(
            changeLog.getHeaders().get(BackfillHeaders.MODE_HEADER_NAME));
  }
}
