package com.linkedin.metadata.service;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * V3 update indices strategy implementation for UpdateIndicesService. This handles the new v3
 * mapping approach with multi-entity indices.
 */
@Slf4j
public class UpdateIndicesV3Strategy implements UpdateIndicesStrategy {

  private final EntityIndexVersionConfiguration v3Config;
  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;
  private final TimeseriesAspectService timeseriesAspectService;
  private final String idHashAlgo;
  private final MultiEntityMappingsBuilder mappingsBuilder;
  private final boolean v2Enabled;

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String idHashAlgo,
      boolean v2Enabled) {
    this.v3Config = v3Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.timeseriesAspectService = timeseriesAspectService;
    this.idHashAlgo = idHashAlgo;
    this.v2Enabled = v2Enabled;
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

    log.debug("Processing {} URN groups with V3 unified batch optimization", groupedEvents.size());

    // Process each group of events for the same URN
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      Urn urn = entry.getKey();
      List<MCLItem> urnEvents = entry.getValue();

      log.debug("Processing {} events for URN: {} with V3 unified batch", urnEvents.size(), urn);

      if (structuredPropertiesHookEnabled) {}

      // V3 optimization: single operation per URN
      processUrnBatch(opContext, urn, urnEvents, structuredPropertiesHookEnabled);
    }
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
                      elasticSearchService.getIndexBuilder().applyMappings(reindexState, false);
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
    return mappingsBuilder.getMappings(opContext);
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
    ObjectNode combinedDocument = buildV3SearchDocument(opContext, urn, events);

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
  }

  /**
   * Builds a V3 search document by combining multiple aspects into a single document with _aspect
   * structure.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   * @return the combined V3 document or null if no aspects to process
   */
  private ObjectNode buildV3SearchDocument(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull List<MCLItem> events) {

    ObjectNode combinedDocument = JsonNodeFactory.instance.objectNode();
    combinedDocument.put("urn", urn.toString());

    // Add _entityType field
    String entityType = events.get(0).getEntitySpec().getName();
    combinedDocument.put("_entityType", entityType);

    // Create _aspects object to hold all aspects
    ObjectNode aspectsNode = JsonNodeFactory.instance.objectNode();

    boolean hasAnyAspects = false;

    for (MCLItem event : events) {
      try {
        AspectSpec aspectSpec = event.getAspectSpec();
        String aspectName = aspectSpec.getName();

        // Handle structured properties specially - they go at root level, not under _aspects
        if (Constants.STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
          processStructuredPropertiesAspect(opContext, urn, event, combinedDocument);
          hasAnyAspects = true;
          continue;
        }

        // Process ALL other aspects (including timeseries) under _aspects
        // In V3, timeseries aspects are treated the same as other versioned aspects
        ObjectNode aspectDocument = processAspectForV3(opContext, urn, event);
        if (aspectDocument != null) {
          aspectsNode.set(aspectName, aspectDocument);
          hasAnyAspects = true;

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

    // Only add _aspects if we have any aspects
    if (hasAnyAspects && aspectsNode.size() > 0) {
      combinedDocument.set(MappingConstants.ASPECTS_FIELD_NAME, aspectsNode);
    }

    return hasAnyAspects ? combinedDocument : null;
  }

  /**
   * Processes a single aspect for V3 document structure.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param event the MCLItem event
   * @return the aspect document or null if no searchable fields
   */
  private ObjectNode processAspectForV3(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull MCLItem event) {

    AspectSpec aspectSpec = event.getAspectSpec();
    RecordTemplate aspect = event.getRecordTemplate();
    SystemMetadata systemMetadata = event.getSystemMetadata();

    try {
      // Transform the aspect using the existing transformer
      // For DELETE events, pass isDelete=true to match V2 behavior
      boolean isDelete = event.getChangeType() == ChangeType.DELETE;
      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer
              .transformAspect(opContext, urn, aspect, aspectSpec, isDelete, event.getAuditStamp())
              .map(
                  objectNode ->
                      SearchDocumentTransformer.withSystemCreated(
                          objectNode,
                          event.getChangeType(),
                          event.getEntitySpec(),
                          aspectSpec,
                          event.getAuditStamp()));

      if (searchDocument.isEmpty()) {
        return null;
      }

      ObjectNode aspectNode = searchDocument.get();

      // Remove the urn field as it's already at the root level
      aspectNode.remove("urn");

      // Add _systemmetadata if present
      if (systemMetadata != null) {
        // Use RecordUtils.toJsonString() for proper serialization, then convert to JsonNode
        String systemMetadataJson = RecordUtils.toJsonString(systemMetadata);
        ObjectMapper mapper = opContext.getObjectMapper();
        JsonNode systemMetadataNode = mapper.readTree(systemMetadataJson);
        aspectNode.set("_systemmetadata", systemMetadataNode);
      }

      return aspectNode;

    } catch (Exception e) {
      log.error(
          "Error transforming aspect {} for URN {}: {}",
          aspectSpec.getName(),
          urn,
          e.getMessage(),
          e);
      return null;
    }
  }

  /**
   * Processes structured properties aspect for V3 (keeps at root level, not under _aspects).
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param event the MCLItem event
   * @param combinedDocument the combined document to add structured properties to
   */
  private void processStructuredPropertiesAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull MCLItem event,
      @Nonnull ObjectNode combinedDocument) {

    try {
      AspectSpec aspectSpec = event.getAspectSpec();
      RecordTemplate aspect = event.getRecordTemplate();

      // Transform structured properties using existing logic
      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer.transformAspect(
              opContext, urn, aspect, aspectSpec, false, event.getAuditStamp());

      if (searchDocument.isPresent()) {
        ObjectNode structuredPropsDoc = searchDocument.get();

        // Copy structured properties fields to root level (excluding urn)
        Iterator<String> fieldNames = structuredPropsDoc.fieldNames();
        if (fieldNames != null) {
          fieldNames.forEachRemaining(
              fieldName -> {
                if (!"urn".equals(fieldName)) {
                  combinedDocument.set(fieldName, structuredPropsDoc.get(fieldName));
                }
              });
        }
      }

    } catch (Exception e) {
      log.error("Error processing structured properties for URN {}: {}", urn, e.getMessage(), e);
    }
  }
}
