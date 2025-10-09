package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.entity.SearchIndicesService;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateIndicesService implements SearchIndicesService {

  @VisibleForTesting @Getter private final UpdateGraphIndicesService updateGraphIndicesService;
  private final ElasticSearchService elasticSearchService;
  private final SystemMetadataService systemMetadataService;

  @Getter private final boolean searchDiffMode;

  @Getter private final boolean structuredPropertiesHookEnabled;

  @Getter private final boolean structuredPropertiesWriteEnabled;

  // Update indices strategies
  private final Collection<UpdateIndicesStrategy> updateStrategies;

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
      ElasticSearchService elasticSearchService,
      TimeseriesAspectService timeseriesAspectService,
      SystemMetadataService systemMetadataService,
      SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull String idHashAlgo) {
    this(
        updateGraphIndicesService,
        elasticSearchService,
        systemMetadataService,
        createDefaultV2Strategy(
            elasticSearchService, searchDocumentTransformer, timeseriesAspectService, idHashAlgo),
        true, // searchDiffMode
        true, // structuredPropertiesHookEnabled
        true); // structuredPropertiesWriteEnabled
  }

  /** Creates a default V2 strategy for backward compatibility. */
  private static Collection<UpdateIndicesStrategy> createDefaultV2Strategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      TimeseriesAspectService timeseriesAspectService,
      String idHashAlgo) {
    EntityIndexVersionConfiguration v2Config =
        EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build();
    UpdateIndicesV2Strategy v2Strategy =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            idHashAlgo);
    return Arrays.asList(v2Strategy);
  }

  public UpdateIndicesService(
      UpdateGraphIndicesService updateGraphIndicesService,
      ElasticSearchService elasticSearchService,
      SystemMetadataService systemMetadataService,
      @Nonnull Collection<UpdateIndicesStrategy> updateStrategies,
      boolean searchDiffMode,
      boolean structuredPropertiesHookEnabled,
      boolean structuredPropertiesWriteEnabled) {
    this.updateGraphIndicesService = updateGraphIndicesService;
    this.elasticSearchService = elasticSearchService;
    this.systemMetadataService = systemMetadataService;
    this.updateStrategies = updateStrategies;
    this.searchDiffMode = searchDiffMode;
    this.structuredPropertiesHookEnabled = structuredPropertiesHookEnabled;
    this.structuredPropertiesWriteEnabled = structuredPropertiesWriteEnabled;
  }

  @Override
  public void handleChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull MetadataChangeLog metadataChangeLog) {
    handleChangeEvents(opContext, Collections.singletonList(metadataChangeLog));
  }

  @Override
  public void handleChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MetadataChangeLog> events) {
    try {
      // Convert MetadataChangeLog events to MCLItem events
      List<MCLItem> mclItems = new ArrayList<>();
      for (MetadataChangeLog event : events) {
        MCLItemImpl batch = MCLItemImpl.builder().build(event, opContext.getAspectRetriever());
        mclItems.add(batch);
      }

      // Apply side effects to all events at once
      Stream<MCLItem> sideEffects =
          AspectsBatch.applyMCLSideEffects(mclItems, opContext.getRetrieverContext());

      // Build combined collection of all events (original + side effects)
      List<MCLItem> allEvents =
          Stream.concat(mclItems.stream(), sideEffects).collect(Collectors.toList());

      // Group all events by URN while preserving order
      LinkedHashMap<Urn, List<MCLItem>> groupedEvents =
          UpdateIndicesUtil.groupEventsByUrn(allEvents.stream());

      // For optimized batch processing we simply process them here
      // and rely on the handleSystemMetadataUpdateChangeEvents method below
      // to process system metadata updates index
      for (UpdateIndicesStrategy strategy : updateStrategies) {
        if (strategy.isEnabled()) {
          strategy.processBatch(opContext, groupedEvents);
        }
      }

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
          // Process regular metadata updates first
          handleUpdateChangeEvents(opContext, updateEvents);

          // Update graph indices for update events
          for (MCLItem event : updateEvents) {
            updateGraphIndicesService.handleChangeEvent(opContext, event.getMetadataChangeLog());
          }

          // Process system metadata updates
          handleSystemMetadataUpdateChangeEvents(opContext, updateEvents);
        }

        // Process delete events
        List<MCLItem> deleteEvents =
            urnEvents.stream()
                .filter(event -> event.getMetadataChangeLog().getChangeType() == ChangeType.DELETE)
                .collect(Collectors.toList());

        for (MCLItem deleteEvent : deleteEvents) {
          Pair<EntitySpec, AspectSpec> specPair = UpdateIndicesUtil.extractSpecPair(deleteEvent);
          boolean isDeletingKey = UpdateIndicesUtil.isDeletingKey(specPair);

          // non-system metadata
          handleDeleteChangeEvent(opContext, specPair, deleteEvent, isDeletingKey);

          // graph update
          updateGraphIndicesService.handleChangeEvent(
              opContext, deleteEvent.getMetadataChangeLog());

          // system metadata is last for tracing
          handleSystemMetadataDeleteChangeEvent(deleteEvent.getUrn(), specPair, isDeletingKey);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles a collection of update change events for regular metadata processing. This method
   * delegates to strategies for efficient batch processing.
   *
   * @param opContext the operation context
   * @param events the collection of update events
   */
  private void handleUpdateChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MCLItem> events)
      throws IOException {

    if (events.isEmpty()) {
      return;
    }

    // Step 0. Handle timeseries aspects using all enabled strategies
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        strategy.updateTimeseriesFields(opContext, events);
      }
    }

    // Step 1. Handle StructuredProperties Index Mapping changes
    for (MCLItem event : events) {
      try {
        updateIndexMappings(
            opContext,
            event.getUrn(),
            event.getEntitySpec(),
            event.getAspectSpec(),
            event.getRecordTemplate(),
            event.getPreviousRecordTemplate());
      } catch (Exception e) {
        log.error("Issue with updating index mappings for structured property change", e);
      }
    }

    // Step 2. Update Search using all enabled strategies
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        strategy.updateSearchIndices(opContext, events);
      }
    }
  }

  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      EntitySpec entitySpec,
      AspectSpec aspectSpec,
      RecordTemplate newValue,
      RecordTemplate oldValue)
      throws CloneNotSupportedException {
    if (structuredPropertiesHookEnabled
        && STRUCTURED_PROPERTY_ENTITY_NAME.equals(entitySpec.getName())
        && STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspectSpec.getName())) {

      // Delegate to all enabled update indices strategies
      for (UpdateIndicesStrategy strategy : updateStrategies) {
        if (strategy.isEnabled()) {
          strategy.updateIndexMappings(opContext, urn, entitySpec, aspectSpec, newValue, oldValue);
        }
      }
    }
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
  private void handleDeleteChangeEvent(
      @Nonnull OperationContext opContext,
      Pair<EntitySpec, AspectSpec> specPair,
      @Nonnull final MCLItem event,
      boolean isDeletingKey) {

    if (!specPair.getSecond().isTimeseries()) {
      // Delegate to all enabled update indices strategies
      for (UpdateIndicesStrategy strategy : updateStrategies) {
        if (strategy.isEnabled()) {
          strategy.deleteSearchData(
              opContext,
              event.getUrn(),
              specPair.getFirst().getName(),
              specPair.getSecond(),
              event.getRecordTemplate(),
              isDeletingKey,
              event.getAuditStamp());
        }
      }
    }
  }

  /**
   * Handles system metadata updates for a collection of update change events. This method processes
   * system metadata separately for tracing purposes.
   *
   * @param opContext the operation context
   * @param events the collection of update events
   */
  private void handleSystemMetadataUpdateChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull final Collection<MCLItem> events) {

    if (events.isEmpty()) {
      return;
    }

    // Handle system metadata for non-timeseries aspects
    for (MCLItem event : events) {
      if (!event.getAspectSpec().isTimeseries()) {
        SystemMetadata systemMetadata = event.getSystemMetadata();
        if (systemMetadata != null) {
          systemMetadataService.insert(
              systemMetadata, event.getUrn().toString(), event.getAspectSpec().getName());

          // If processing status aspect update all aspects for this urn to removed
          if (event.getAspectSpec().getName().equals(Constants.STATUS_ASPECT_NAME)) {
            RecordTemplate aspect = event.getRecordTemplate();
            if (aspect instanceof Status) {
              systemMetadataService.setDocStatus(
                  event.getUrn().toString(), ((Status) aspect).isRemoved());
            }
          }
        }
      }
    }
  }

  /**
   * Handle the system metadata deletion separately for tracing
   *
   * @param urn delete urn
   * @param specPair entity & aspect spec
   * @param isDeletingKey whether the key aspect is being deleted
   */
  private void handleSystemMetadataDeleteChangeEvent(
      @Nonnull Urn urn, Pair<EntitySpec, AspectSpec> specPair, boolean isDeletingKey) {
    if (!specPair.getSecond().isTimeseries()) {
      if (isDeletingKey) {
        // Delete all aspects
        log.debug(String.format("Deleting all system metadata for urn: %s", urn));
        systemMetadataService.deleteUrn(urn.toString());
      } else {
        // Delete all aspects from system metadata service
        log.debug(
            String.format(
                "Deleting system metadata for urn: %s, aspect: %s",
                urn, specPair.getSecond().getName()));
        systemMetadataService.deleteAspect(urn.toString(), specPair.getSecond().getName());
      }
    }
  }

  /**
   * Flushes any pending operations in the bulk processor to ensure all data is written to
   * Elasticsearch. This is particularly important for loadIndices operations where we want to
   * ensure all data is persisted.
   */
  public void flush() {
    try {
      // Access the bulk processor through the ElasticSearchService's ESWriteDAO
      ESWriteDAO writeDAO = elasticSearchService.getEsWriteDAO();
      ESBulkProcessor bulkProcessor = writeDAO.getBulkProcessor();

      bulkProcessor.flush();
      log.info("Successfully flushed bulk processor");
    } catch (Exception e) {
      log.error("Failed to flush bulk processor", e);
      throw new RuntimeException("Failed to flush bulk processor", e);
    }
  }
}
