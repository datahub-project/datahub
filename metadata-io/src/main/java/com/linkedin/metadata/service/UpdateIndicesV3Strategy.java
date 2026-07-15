package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;
import static com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder.IS_EXPLODED_FIELD;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.search.CombinedSearchDocumentBuilder;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.timeseries.write.TimeseriesAspectWriteSink;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * V3 update indices strategy implementation for UpdateIndicesService. This handles the new v3
 * mapping approach with multi-entity indices.
 *
 * <p>Timeseries Elasticsearch and optional PostgreSQL writes mirror {@link UpdateIndicesV2Strategy}
 * only when {@code elasticsearch.entityIndex.v2.enabled} is false; when V2 is enabled, V2 owns
 * those paths and V3 avoids duplicate writes.
 */
@Slf4j
public class UpdateIndicesV3Strategy implements UpdateIndicesStrategy {

  private final EntityIndexVersionConfiguration v3Config;
  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;
  private final TimeseriesAspectService timeseriesAspectService;
  private final TimeseriesAspectWriteSink timeseriesAspectWriteSink;
  private final String idHashAlgo;
  private final MultiEntityMappingsBuilder mappingsBuilder;
  private final boolean v2Enabled;
  private final CombinedSearchDocumentBuilder combinedSearchDocumentBuilder;

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull TimeseriesAspectWriteSink timeseriesAspectWriteSink,
      @Nonnull String idHashAlgo,
      boolean v2Enabled) {
    this.v3Config = v3Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.timeseriesAspectService = timeseriesAspectService;
    this.timeseriesAspectWriteSink = timeseriesAspectWriteSink;
    this.idHashAlgo = idHashAlgo;
    this.v2Enabled = v2Enabled;
    this.combinedSearchDocumentBuilder =
        new CombinedSearchDocumentBuilder(searchDocumentTransformer);
    try {
      this.mappingsBuilder =
          new MultiEntityMappingsBuilder(EntityIndexConfiguration.builder().v3(v3Config).build());
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

    log.debug("Processing {} URN groups with V3 unified batch optimization", groupedEvents.size());

    // Process each group of events for the same URN
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      Urn urn = entry.getKey();
      List<MCLItem> urnEvents = entry.getValue();

      log.debug("Processing {} events for URN: {} with V3 unified batch", urnEvents.size(), urn);

      if (!v2Enabled) {
        processTimeseriesAspectEventsForUrnGroup(opContext, urnEvents);
      }

      // V3 optimization: single operation per URN
      processUrnBatch(opContext, urn, urnEvents, structuredPropertiesHookEnabled);
    }
  }

  /**
   * When V2 is disabled, V3 must drive dedicated timeseries indices (and optional Postgres sink)
   * the same way V2 does. When V2 is enabled, that strategy already handles these writes.
   */
  private void processTimeseriesAspectEventsForUrnGroup(
      @Nonnull OperationContext opContext, @Nonnull List<MCLItem> urnEvents) {
    List<MCLItem> updateEvents =
        urnEvents.stream()
            .filter(event -> UPDATE_CHANGE_TYPES.contains(event.getChangeType()))
            .collect(Collectors.toList());
    for (MCLItem event : updateEvents) {
      try {
        updateTimeseriesFieldsForEvent(opContext, event);
      } catch (Exception e) {
        log.error(
            "V3 timeseries update failed for urn {} aspect {}: {}",
            event.getUrn(),
            event.getAspectName(),
            e.getMessage(),
            e);
      }
    }

    List<MCLItem> deleteEvents =
        urnEvents.stream()
            .filter(event -> event.getChangeType() == ChangeType.DELETE)
            .collect(Collectors.toList());
    for (MCLItem deleteEvent : deleteEvents) {
      try {
        Pair<EntitySpec, AspectSpec> specPair = UpdateIndicesUtil.extractSpecPair(deleteEvent);
        if (specPair.getSecond().isTimeseries()) {
          deleteTimeseriesFieldsForDeleteEvent(opContext, deleteEvent);
        }
      } catch (Exception e) {
        log.error(
            "V3 timeseries delete handling failed for urn {} aspect {}: {}",
            deleteEvent.getUrn(),
            deleteEvent.getAspectName(),
            e.getMessage(),
            e);
      }
    }
  }

  private void deleteTimeseriesFieldsForDeleteEvent(
      @Nonnull OperationContext opContext, @Nonnull MCLItem deleteEvent) {
    AspectSpec aspectSpec = deleteEvent.getAspectSpec();
    if (!aspectSpec.isTimeseries()) {
      return;
    }
    RecordTemplate previous = deleteEvent.getPreviousRecordTemplate();
    if (previous == null) {
      log.debug(
          "Timeseries delete has no previous aspect snapshot; skipping timeseries index delete for urn {} aspect {}",
          deleteEvent.getUrn(),
          aspectSpec.getName());
      return;
    }
    Urn urn = deleteEvent.getUrn();
    String entityType = deleteEvent.getEntitySpec().getName();
    String aspectName = aspectSpec.getName();
    SystemMetadata prevSys =
        deleteEvent.getPreviousSystemMetadata() != null
            ? deleteEvent.getPreviousSystemMetadata()
            : deleteEvent.getSystemMetadata();
    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(urn, previous, aspectSpec, prevSys, idHashAlgo);
    } catch (JsonProcessingException e) {
      log.error(
          "Failed to resolve timeseries documents for delete event for urn {} aspect {}: {}",
          urn,
          aspectName,
          e.toString());
      return;
    }
    for (Map.Entry<String, JsonNode> entry : documents.entrySet()) {
      JsonNode doc = entry.getValue();
      boolean exploded = doc.has(IS_EXPLODED_FIELD) && doc.get(IS_EXPLODED_FIELD).asBoolean(false);
      timeseriesAspectService.deleteDocument(
          opContext, entityType, aspectName, entry.getKey(), exploded);
      timeseriesAspectWriteSink.deleteDocument(
          opContext, entityType, aspectName, entry.getKey(), doc, exploded);
    }
  }

  private void updateTimeseriesFieldsForEvent(
      @Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    log.debug(
        "Updating V3 timeseries fields for entity: {} aspect: {}",
        event.getUrn(),
        event.getAspectName());

    Urn urn = event.getUrn();
    String entityType = event.getEntitySpec().getName();
    String aspectName = event.getAspectName();
    Object aspect = event.getRecordTemplate();
    AspectSpec aspectSpec = event.getAspectSpec();
    SystemMetadata systemMetadata = event.getSystemMetadata();

    if (!aspectSpec.isTimeseries()) {
      log.debug("Aspect {} is not timeseries, skipping V3 timeseries update", aspectName);
      return;
    }

    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(
              urn, (RecordTemplate) aspect, aspectSpec, systemMetadata, idHashAlgo);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate V3 timeseries document from aspect: {}", e.toString());
      return;
    }

    documents
        .entrySet()
        .forEach(
            document -> {
              timeseriesAspectService.upsertDocument(
                  opContext, entityType, aspectName, document.getKey(), document.getValue());
              timeseriesAspectWriteSink.upsertDocument(
                  opContext, entityType, aspectName, document.getKey(), document.getValue());
            });
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

        UrnArray oldEntityTypes =
            Optional.ofNullable(oldValue)
                .map(
                    recordTemplate ->
                        new StructuredPropertyDefinition(((RecordTemplate) recordTemplate).data())
                            .getEntityTypes())
                .orElse(new UrnArray());

        StructuredPropertyDefinition newDefinition =
            new StructuredPropertyDefinition(((RecordTemplate) newValue).data().copy());
        newDefinition.getEntityTypes().removeAll(oldEntityTypes);

        if (newDefinition.getEntityTypes().size() > 0) {

          // V3 uses the same approach as V2 but with V3 mappings and index convention
          log.info(
              "V3 structured property mapping update for {} - updating indices", newDefinition);

          elasticSearchService
              .buildReindexConfigsWithNewStructProp(opContext, urn, newDefinition)
              .forEach(
                  reindexState -> {
                    try {
                      log.info(
                          "Applying new V3 structured property {} to index {}",
                          newDefinition,
                          reindexState.name());
                      elasticSearchService
                          .getIndexBuilder()
                          .applyMappings(opContext, reindexState, false);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
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
    // V3 structured property mapping logic - stub for now
    log.debug("Getting V3 index mappings with new structured property: {}", urn);
    return Collections.emptyList();
  }

  @Override
  public boolean isEnabled() {
    return v3Config.isEnabled();
  }

  /**
   * Processes all events for a single URN in V3 optimized fashion. Either deletes the entire entity
   * or performs a single upsert.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   */
  private void processUrnBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      boolean structuredPropertiesHookEnabled) {

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

    if (hasKeyAspectDeletion) {
      // Delete the entire document for key aspect deletion

      String searchGroup = events.get(0).getEntitySpec().getSearchGroup();
      if (searchGroup == null) {
        log.error("V3 key aspect deletion detected but search group is null for URN: {}", urn);
        return;
      }

      String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
      elasticSearchService.deleteDocumentBySearchGroup(opContext, searchGroup, docId);
      log.debug(
          "V3 deleted entire document for URN: {} from search group: {} due to key aspect deletion",
          urn,
          searchGroup);
      return;
    }

    // Build the combined V3 document with _aspect structure
    ObjectNode combinedDocument =
        combinedSearchDocumentBuilder.buildCombinedDocument(opContext, urn, events);

    if (combinedDocument == null) {
      log.debug("V3 combined document is empty for URN: {}, skipping update", urn);
      return;
    }

    // Write the combined document to Elasticsearch
    if (events.isEmpty()) {
      log.warn("V3 upsert attempted but no events available for URN: {}", urn);
      return;
    }

    String searchGroup = events.get(0).getEntitySpec().getSearchGroup();
    if (searchGroup == null) {
      log.error("V3 upsert attempted but search group is null for URN: {}", urn);
      return;
    }

    String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
    String finalDocument = combinedDocument.toString();

    if (structuredPropertiesHookEnabled) {
      for (MCLItem event : events) {
        EntitySpec entitySpec = event.getEntitySpec();
        AspectSpec aspectSpec = event.getAspectSpec();
        updateIndexMappings(
            opContext,
            event.getUrn(),
            entitySpec,
            aspectSpec,
            event.getRecordTemplate(),
            event.getPreviousRecordTemplate());
      }
    }
    elasticSearchService.upsertDocumentBySearchGroup(opContext, searchGroup, finalDocument, docId);
    log.debug(
        "V3 upserted combined document for URN: {} to search group: {} with {} aspects",
        urn,
        searchGroup,
        events.size());

    // Append runIds to search document so rollback/list runs can find touched URNs (MAE path)
    List<String> distinctRunIds =
        events.stream()
            .filter(e -> e.getSystemMetadata() != null && e.getSystemMetadata().hasRunId())
            .map(e -> e.getSystemMetadata().getRunId())
            .distinct()
            .collect(Collectors.toList());
    for (String runId : distinctRunIds) {
      elasticSearchService.appendRunIdBySearchGroup(opContext, searchGroup, docId, urn, runId);
    }
  }
}
