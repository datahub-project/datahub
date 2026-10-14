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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
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
 * <p>Two consequences are deliberate. An entity whose key aspect is absent from storage is treated
 * as gone even if other aspect rows remain, so such a legacy entity is removed from the index on
 * replay. And a storage read that fails propagates out of the whole batch: a replay is the only
 * pass over its window, so the failure must surface rather than drop the URN, and the live events
 * grouped into the same batch are redelivered with it.
 */
@Slf4j
public final class CatchUpBatchRebuilder {

  /** URNs per storage read; bounds the query and the entities held in memory at once. */
  private static final int READ_SLICE_SIZE = 500;

  /** The batches to process in place of {@code groupedEvents}; a batch without the mark is kept. */
  @Nonnull
  public Map<Urn, List<MCLItem>> rebuild(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, List<MCLItem>> groupedEvents) {
    Map<Urn, Set<String>> storedAspectNames = new HashMap<>();
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      if (entry.getValue().stream().anyMatch(CatchUpBatchRebuilder::isCatchUpEvent)) {
        storedAspectNames.put(
            entry.getKey(), storedAspectNames(entry.getValue().get(0).getEntitySpec()));
      }
    }
    if (storedAspectNames.isEmpty()) {
      return groupedEvents;
    }
    OperationContext primary = primaryStorageContext(opContext);
    Map<Urn, Map<String, SystemAspect>> stored = new HashMap<>();
    for (Map<Urn, Set<String>> read : reads(storedAspectNames)) {
      stored.putAll(
          Optional.ofNullable(primary.getAspectRetriever().getLatestSystemAspects(primary, read))
              .orElse(Map.of()));
    }
    Map<Urn, List<MCLItem>> rebuilt = new LinkedHashMap<>(groupedEvents);
    for (Urn urn : storedAspectNames.keySet()) {
      rebuilt.put(
          urn,
          storedStateEvents(
              opContext, urn, groupedEvents.get(urn), stored.getOrDefault(urn, Map.of())));
    }
    return rebuilt;
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
    if (storedAspects.get(entitySpec.getKeyAspectName()) == null) {
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
        .filter(entry -> entry.getValue() != null && entry.getValue().getRecordTemplate() != null)
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
