package com.linkedin.metadata.service;

import static com.linkedin.metadata.search.transformer.SearchDocumentTransformer.withSystemCreated;
import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * V2 update indices strategy implementation for UpdateIndicesService. This handles the legacy v2
 * mapping approach with per-entity indices.
 */
@Slf4j
public class UpdateIndicesV2Strategy implements UpdateIndicesStrategy {

  private final EntityIndexVersionConfiguration v2Config;
  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;
  private final TimeseriesAspectService timeseriesAspectService;
  private final String idHashAlgo;
  private final V2MappingsBuilder mappingsBuilder;

  public UpdateIndicesV2Strategy(
      @Nonnull EntityIndexVersionConfiguration v2Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String idHashAlgo) {
    this.v2Config = v2Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.timeseriesAspectService = timeseriesAspectService;
    this.idHashAlgo = idHashAlgo;
    this.mappingsBuilder =
        new V2MappingsBuilder(
            com.linkedin.metadata.config.search.EntityIndexConfiguration.builder()
                .v2(v2Config)
                .build());
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    // Process each group of events for the same URN
    for (List<MCLItem> urnEvents : groupedEvents.values()) {

      // Process update events
      List<MCLItem> updateEvents =
          urnEvents.stream()
              .filter(
                  event ->
                      UPDATE_CHANGE_TYPES.contains(event.getMetadataChangeLog().getChangeType()))
              .collect(Collectors.toList());

      if (!updateEvents.isEmpty()) {
        updateEvents.forEach(
            event -> {
              if (structuredPropertiesHookEnabled) {
                updateIndexMappings(opContext, event);
              }
              updateSearchIndicesForEvent(opContext, event);
              updateTimeseriesFieldsForEvent(opContext, event);
            });
      }

      // Process delete events
      List<MCLItem> deleteEvents =
          urnEvents.stream()
              .filter(event -> event.getMetadataChangeLog().getChangeType() == ChangeType.DELETE)
              .collect(Collectors.toList());

      for (MCLItem deleteEvent : deleteEvents) {
        Pair<EntitySpec, AspectSpec> specPair = UpdateIndicesUtil.extractSpecPair(deleteEvent);
        boolean isDeletingKey = UpdateIndicesUtil.isDeletingKey(specPair);

        if (!specPair.getSecond().isTimeseries()) {
          deleteSearchData(
              opContext,
              deleteEvent.getUrn(),
              specPair.getFirst().getName(),
              specPair.getSecond(),
              deleteEvent.getRecordTemplate(),
              isDeletingKey,
              deleteEvent.getAuditStamp());
        }
      }
    }
  }

  void updateSearchIndicesForEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    // V2 search index update logic - full implementation
    log.debug("Updating V2 search indices for entity: {}", event.getUrn());

    Urn urn = event.getUrn();
    RecordTemplate aspect = event.getRecordTemplate();
    AspectSpec aspectSpec = event.getAspectSpec();
    SystemMetadata systemMetadata = event.getSystemMetadata();
    RecordTemplate previousAspect = event.getPreviousRecordTemplate();
    String entityName = event.getEntitySpec().getName();

    Optional<ObjectNode> searchDocument;
    Optional<ObjectNode> previousSearchDocument = Optional.empty();
    try {
      searchDocument =
          searchDocumentTransformer
              .transformAspect(opContext, urn, aspect, aspectSpec, false, event.getAuditStamp())
              .map(
                  objectNode ->
                      withSystemCreated(
                          objectNode,
                          event.getChangeType(),
                          event.getEntitySpec(),
                          aspectSpec,
                          event.getAuditStamp()));
    } catch (Exception e) {
      log.error(
          "Error in getting documents for urn: {} from aspect: {}", urn, aspectSpec.getName(), e);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(this.getClass(), "document_transform_failed", 1));
      return;
    }

    if (searchDocument.isEmpty()) {
      log.debug("Search document for urn: {} aspect: {} was empty", urn, aspect);
      return;
    }

    final String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);

    // V2 search diff mode logic
    if (v2Config.isEnabled() // Use v2 config to determine if diff mode is enabled
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get("FORCE_INDEXING")))) {
      if (previousAspect != null) {
        try {
          previousSearchDocument =
              searchDocumentTransformer.transformAspect(
                  opContext, urn, previousAspect, aspectSpec, false, event.getAuditStamp());
        } catch (Exception e) {
          log.error(
              "Error in getting documents from previous aspect state for urn: {} for aspect {}, continuing without diffing.",
              urn,
              aspectSpec.getName(),
              e);
          opContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(this.getClass(), "document_transform_failed", 1));
        }
      }

      if (previousSearchDocument.isPresent()) {
        if (searchDocument.get().toString().equals(previousSearchDocument.get().toString())) {
          // No changes to search document, skip writing no-op update
          log.info(
              "No changes detected for V2 search document for urn: {} aspect: {}",
              urn,
              aspectSpec.getName());
          opContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(this.getClass(), "search_diff_no_changes_detected", 1));
          return;
        }
      }
    }

    String finalDocument =
        SearchDocumentTransformer.handleRemoveFields(
                searchDocument.get(), previousSearchDocument.orElse(null))
            .toString();

    elasticSearchService.upsertDocument(opContext, entityName, finalDocument, docId);
  }

  void deleteSearchData(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate aspect,
      @Nonnull Boolean isKeyAspect,
      @Nonnull AuditStamp auditStamp) {
    // V2 search data deletion logic
    log.debug("Deleting V2 search data for entity: {} aspect: {}", urn, aspectSpec.getName());

    String docId;
    try {
      docId = URLEncoder.encode(urn.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to encode the urn with error: {}", e.toString());
      return;
    }

    if (isKeyAspect) {
      elasticSearchService.deleteDocument(opContext, entityName, docId);
      return;
    }

    Optional<String> searchDocument;
    try {
      searchDocument =
          searchDocumentTransformer
              .transformAspect(opContext, urn, aspect, aspectSpec, true, auditStamp)
              .map(Objects::toString);
    } catch (Exception e) {
      log.error(
          "Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    elasticSearchService.upsertDocument(opContext, entityName, searchDocument.get(), docId);
  }

  void updateTimeseriesFieldsForEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    // V2 timeseries update logic - uses the existing TimeseriesAspectTransformer
    log.debug(
        "Updating V2 timeseries fields for entity: {} aspect: {}",
        event.getUrn(),
        event.getAspectName());

    Urn urn = event.getUrn();
    String entityType = event.getEntitySpec().getName();
    String aspectName = event.getAspectName();
    Object aspect = event.getRecordTemplate();
    AspectSpec aspectSpec = event.getAspectSpec();
    SystemMetadata systemMetadata = event.getSystemMetadata();

    // Check if aspect is timeseries before processing
    if (!aspectSpec.isTimeseries()) {
      log.debug("Aspect {} is not timeseries, skipping V2 timeseries update", aspectName);
      return;
    }

    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(
              urn, (RecordTemplate) aspect, aspectSpec, systemMetadata, idHashAlgo);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate V2 timeseries document from aspect: {}", e.toString());
      return;
    }

    documents
        .entrySet()
        .forEach(
            document -> {
              timeseriesAspectService.upsertDocument(
                  opContext, entityType, aspectName, document.getKey(), document.getValue());
            });
  }

  void updateIndexMappings(OperationContext opContext, MCLItem event) {
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

  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    try {
      // V2 structured property mapping update logic
      log.debug("Updating V2 index mappings for structured property change: {}", urn);

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
          elasticSearchService
              .buildReindexConfigsWithNewStructProp(opContext, urn, newDefinition)
              .forEach(
                  reindexState -> {
                    try {
                      log.info(
                          "Applying new V2 structured property {} to index {}",
                          newDefinition,
                          reindexState.name());
                      elasticSearchService.getIndexBuilder().applyMappings(reindexState, false);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });
        }
      }
    } catch (Exception e) {
      log.error("Issue with updating V2 index mappings for structured property change", e);
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
    return v2Config.isEnabled();
  }

  // Package-level methods for testing
  void updateSearchIndices(
      @Nonnull OperationContext opContext, @Nonnull Collection<MCLItem> events) {
    // V2 strategy processes events individually for backward compatibility
    // This allows V2 to work with the new collection interface while maintaining existing behavior
    for (MCLItem event : events) {
      updateSearchIndicesForEvent(opContext, event);
    }
  }

  void updateTimeseriesFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<MCLItem> events) {
    // V2 strategy processes events individually for backward compatibility
    // This allows V2 to work with the new collection interface while maintaining existing behavior
    for (MCLItem event : events) {
      updateTimeseriesFieldsForEvent(opContext, event);
    }
  }
}
