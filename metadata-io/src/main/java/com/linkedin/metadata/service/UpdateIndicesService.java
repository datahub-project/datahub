package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.transformer.SearchDocumentTransformer.withSystemCreated;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.SearchIndicesService;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateIndicesService implements SearchIndicesService {

  @VisibleForTesting @Getter private final UpdateGraphIndicesService updateGraphIndicesService;
  private final EntitySearchService entitySearchService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final SystemMetadataService systemMetadataService;
  private final SearchDocumentTransformer searchDocumentTransformer;
  private final EntityIndexBuilders entityIndexBuilders;
  @Nonnull private final String idHashAlgo;

  @Getter private final boolean searchDiffMode;

  @Getter private final boolean structuredPropertiesHookEnabled;

  @Getter private final boolean structuredPropertiesWriteEnabled;

  private static final String DOCUMENT_TRANSFORM_FAILED_METRIC = "document_transform_failed";
  private static final String SEARCH_DIFF_MODE_SKIPPED_METRIC = "search_diff_no_changes_detected";

  private static final Set<ChangeType> UPDATE_CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  public UpdateIndicesService(
      UpdateGraphIndicesService updateGraphIndicesService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityIndexBuilders entityIndexBuilders,
      @Nonnull String idHashAlgo) {
    this(
        updateGraphIndicesService,
        entitySearchService,
        timeseriesAspectService,
        systemMetadataService,
        searchDocumentTransformer,
        entityIndexBuilders,
        idHashAlgo,
        true,
        true,
        true);
  }

  public UpdateIndicesService(
      UpdateGraphIndicesService updateGraphIndicesService,
      EntitySearchService entitySearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityIndexBuilders entityIndexBuilders,
      @Nonnull String idHashAlgo,
      boolean searchDiffMode,
      boolean structuredPropertiesHookEnabled,
      boolean structuredPropertiesWriteEnabled) {
    this.updateGraphIndicesService = updateGraphIndicesService;
    this.entitySearchService = entitySearchService;
    this.timeseriesAspectService = timeseriesAspectService;
    this.systemMetadataService = systemMetadataService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.entityIndexBuilders = entityIndexBuilders;
    this.idHashAlgo = idHashAlgo;
    this.searchDiffMode = searchDiffMode;
    this.structuredPropertiesHookEnabled = structuredPropertiesHookEnabled;
    this.structuredPropertiesWriteEnabled = structuredPropertiesWriteEnabled;
  }

  @Override
  public void handleChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull final MetadataChangeLog event) {
    try {
      MCLItemImpl batch = MCLItemImpl.builder().build(event, opContext.getAspectRetriever());

      Stream<MCLItem> sideEffects =
          AspectsBatch.applyMCLSideEffects(List.of(batch), opContext.getRetrieverContext());

      for (MCLItem mclItem :
          Stream.concat(Stream.of(batch), sideEffects).collect(Collectors.toList())) {
        MetadataChangeLog hookEvent = mclItem.getMetadataChangeLog();
        if (UPDATE_CHANGE_TYPES.contains(hookEvent.getChangeType())) {
          // non-system metadata
          handleUpdateChangeEvent(opContext, mclItem, false);
          // graph update
          updateGraphIndicesService.handleChangeEvent(opContext, event);
          // system metadata is last for tracing
          handleUpdateChangeEvent(opContext, mclItem, true);
        } else if (hookEvent.getChangeType() == ChangeType.DELETE) {
          Pair<EntitySpec, AspectSpec> specPair = extractSpecPair(mclItem);
          boolean isDeletingKey = isDeletingKey(specPair);

          // non-system metadata
          handleNonSystemMetadataDeleteChangeEvent(opContext, specPair, mclItem, isDeletingKey);
          // graph update
          updateGraphIndicesService.handleChangeEvent(opContext, event);
          // system metadata is last for tracing
          handleSystemMetadataDeleteChangeEvent(mclItem.getUrn(), specPair, isDeletingKey);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This very important method processes {@link MetadataChangeLog} events that represent changes to
   * the Metadata Graph.
   *
   * <p>In particular, it handles updating the Search, Graph, Timeseries, and System Metadata stores
   * in response to a given change type to reflect the changes present in the new aspect.
   *
   * @param event the change event to be processed.
   */
  private void handleUpdateChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull final MCLItem event, boolean forSystemMetadata)
      throws IOException {

    final EntitySpec entitySpec = event.getEntitySpec();
    final AspectSpec aspectSpec = event.getAspectSpec();
    final Urn urn = event.getUrn();

    RecordTemplate aspect = event.getRecordTemplate();
    RecordTemplate previousAspect = event.getPreviousRecordTemplate();

    if (!forSystemMetadata) {
      // Step 0. If the aspect is timeseries, add to its timeseries index.
      if (aspectSpec.isTimeseries()) {
        updateTimeseriesFields(
            opContext,
            urn.getEntityType(),
            event.getAspectName(),
            urn,
            aspect,
            aspectSpec,
            event.getSystemMetadata());
      }

      try {
        // Step 1. Handle StructuredProperties Index Mapping changes
        updateIndexMappings(urn, entitySpec, aspectSpec, aspect, previousAspect);
      } catch (Exception e) {
        log.error("Issue with updating index mappings for structured property change", e);
      }

      // Step 2. For all aspects, attempt to update Search
      updateSearchService(opContext, event);
    } else if (forSystemMetadata && !aspectSpec.isTimeseries()) {
      // Inject into the System Metadata Index when an aspect is non-timeseries only.
      // TODO: Verify whether timeseries aspects can be dropped into System Metadata as well
      // without impacting rollbacks.
      updateSystemMetadata(event.getSystemMetadata(), urn, aspectSpec, aspect);
    }
  }

  public void updateIndexMappings(
      @Nonnull Urn urn,
      EntitySpec entitySpec,
      AspectSpec aspectSpec,
      RecordTemplate newValue,
      RecordTemplate oldValue)
      throws CloneNotSupportedException {
    if (structuredPropertiesHookEnabled
        && STRUCTURED_PROPERTY_ENTITY_NAME.equals(entitySpec.getName())
        && STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspectSpec.getName())) {

      UrnArray oldEntityTypes =
          Optional.ofNullable(oldValue)
              .map(
                  recordTemplate ->
                      new StructuredPropertyDefinition(recordTemplate.data()).getEntityTypes())
              .orElse(new UrnArray());

      StructuredPropertyDefinition newDefinition =
          new StructuredPropertyDefinition(newValue.data().copy());
      newDefinition.getEntityTypes().removeAll(oldEntityTypes);

      if (newDefinition.getEntityTypes().size() > 0) {
        entityIndexBuilders
            .buildReindexConfigsWithNewStructProp(urn, newDefinition)
            .forEach(
                reindexState -> {
                  try {
                    log.info(
                        "Applying new structured property {} to index {}",
                        newDefinition,
                        reindexState.name());
                    entityIndexBuilders.getIndexBuilder().applyMappings(reindexState, false);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
      }
    }
  }

  private static Pair<EntitySpec, AspectSpec> extractSpecPair(@Nonnull final MCLItem event) {
    final EntitySpec entitySpec = event.getEntitySpec();
    final Urn urn = event.getUrn();

    AspectSpec aspectSpec = entitySpec.getAspectSpec(event.getAspectName());
    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Aspect Spec for entity with name %s, aspect with name %s. Cannot update indices for MCL.",
              urn.getEntityType(), event.getAspectName()));
    }

    return Pair.of(entitySpec, aspectSpec);
  }

  private static boolean isDeletingKey(Pair<EntitySpec, AspectSpec> specPair) {
    return specPair.getSecond().getName().equals(specPair.getFirst().getKeyAspectName());
  }

  /**
   * This very important method processes {@link MetadataChangeLog} deletion events to cleanup the
   * Metadata Graph when an aspect or entity is removed.
   *
   * <p>In particular, it handles updating the Search, Graph, Timeseries, and System Metadata stores
   * to reflect the deletion of a particular aspect.
   *
   * <p>Note that if an entity's key aspect is deleted, the entire entity will be purged from
   * search, graph, timeseries, etc.
   *
   * @param opContext operation's context
   * @param specPair entity & aspect spec
   * @param event the change event to be processed.
   * @param isDeletingKey whether the key aspect is being deleted
   */
  private void handleNonSystemMetadataDeleteChangeEvent(
      @Nonnull OperationContext opContext,
      Pair<EntitySpec, AspectSpec> specPair,
      @Nonnull final MCLItem event,
      boolean isDeletingKey) {

    if (!specPair.getSecond().isTimeseries()) {
      deleteSearchData(
          opContext,
          event.getUrn(),
          specPair.getFirst().getName(),
          specPair.getSecond(),
          event.getRecordTemplate(),
          isDeletingKey,
          event.getAuditStamp());
    }
  }

  /**
   * Handle the system metadata separately for tracing
   *
   * @param urn delete urn
   * @param specPair entity & aspect spec
   * @param isDeletingKey whether the key aspect is being deleted
   */
  private void handleSystemMetadataDeleteChangeEvent(
      @Nonnull Urn urn, Pair<EntitySpec, AspectSpec> specPair, boolean isDeletingKey) {
    if (!specPair.getSecond().isTimeseries()) {
      deleteSystemMetadata(urn, specPair.getSecond(), isDeletingKey);
    }
  }

  /** Process snapshot and update search index */
  private void updateSearchService(@Nonnull OperationContext opContext, MCLItem event) {
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
      MetricUtils.counter(this.getClass(), DOCUMENT_TRANSFORM_FAILED_METRIC).inc();
      return;
    }

    if (searchDocument.isEmpty()) {
      log.info("Search document for urn: {} aspect: {} was empty", urn, aspect);
      return;
    }

    final String docId = entityIndexBuilders.getIndexConvention().getEntityDocumentId(urn);

    if (searchDiffMode
        && (systemMetadata == null
            || systemMetadata.getProperties() == null
            || !Boolean.parseBoolean(systemMetadata.getProperties().get(FORCE_INDEXING_KEY)))) {
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
          MetricUtils.counter(this.getClass(), DOCUMENT_TRANSFORM_FAILED_METRIC).inc();
        }
      }

      if (previousSearchDocument.isPresent()) {
        if (searchDocument.get().toString().equals(previousSearchDocument.get().toString())) {
          // No changes to search document, skip writing no-op update
          log.info(
              "No changes detected for search document for urn: {} aspect: {}",
              urn,
              aspectSpec.getName());
          MetricUtils.counter(this.getClass(), SEARCH_DIFF_MODE_SKIPPED_METRIC).inc();
          return;
        }
      }
    }

    String finalDocument =
        SearchDocumentTransformer.handleRemoveFields(
                searchDocument.get(), previousSearchDocument.orElse(null))
            .toString();

    entitySearchService.upsertDocument(opContext, entityName, finalDocument, docId);
  }

  /** Process snapshot and update time-series index */
  private void updateTimeseriesFields(
      @Nonnull OperationContext opContext,
      String entityType,
      String aspectName,
      Urn urn,
      RecordTemplate aspect,
      AspectSpec aspectSpec,
      SystemMetadata systemMetadata) {
    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(
              urn, aspect, aspectSpec, systemMetadata, idHashAlgo);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
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

  private void updateSystemMetadata(
      SystemMetadata systemMetadata, Urn urn, AspectSpec aspectSpec, RecordTemplate aspect) {
    systemMetadataService.insert(systemMetadata, urn.toString(), aspectSpec.getName());

    // If processing status aspect update all aspects for this urn to removed
    if (aspectSpec.getName().equals(Constants.STATUS_ASPECT_NAME)) {
      systemMetadataService.setDocStatus(urn.toString(), ((Status) aspect).isRemoved());
    }
  }

  private void deleteSystemMetadata(Urn urn, AspectSpec aspectSpec, Boolean isKeyAspect) {
    if (isKeyAspect) {
      // Delete all aspects
      log.debug(String.format("Deleting all system metadata for urn: %s", urn));
      systemMetadataService.deleteUrn(urn.toString());
    } else {
      // Delete all aspects from system metadata service
      log.debug(
          String.format(
              "Deleting system metadata for urn: %s, aspect: %s", urn, aspectSpec.getName()));
      systemMetadataService.deleteAspect(urn.toString(), aspectSpec.getName());
    }
  }

  private void deleteSearchData(
      @Nonnull OperationContext opContext,
      Urn urn,
      String entityName,
      AspectSpec aspectSpec,
      @Nullable RecordTemplate aspect,
      Boolean isKeyAspect,
      AuditStamp auditStamp) {
    String docId;
    try {
      docId = URLEncoder.encode(urn.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to encode the urn with error: {}", e.toString());
      return;
    }

    if (isKeyAspect) {
      entitySearchService.deleteDocument(opContext, entityName, docId);
      return;
    }

    Optional<String> searchDocument;
    try {
      searchDocument =
          searchDocumentTransformer
              .transformAspect(opContext, urn, aspect, aspectSpec, true, auditStamp)
              .map(Objects::toString); // TODO
    } catch (Exception e) {
      log.error(
          "Error in getting documents from aspect: {} for aspect {}", e, aspectSpec.getName());
      return;
    }

    if (!searchDocument.isPresent()) {
      return;
    }

    entitySearchService.upsertDocument(opContext, entityName, searchDocument.get(), docId);
  }
}
