package com.linkedin.metadata.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
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
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Coordinates metadata-change side effects for search indexing: pluggable {@link
 * UpdateIndicesStrategy} implementations (OpenSearch, PostgreSQL entity search, etc.), graph
 * updates, and system metadata. OpenSearch bulk flush is {@link
 * com.linkedin.metadata.search.elasticsearch.ElasticSearchService#flush()}, not here.
 */
@Slf4j
public class UpdateIndicesService implements SearchIndicesService {

  @VisibleForTesting @Getter private final UpdateGraphIndicesService updateGraphIndicesService;
  private final SystemMetadataService systemMetadataService;

  @Getter private final boolean searchDiffMode;

  @Getter private final boolean structuredPropertiesHookEnabled;

  @Getter private final boolean structuredPropertiesWriteEnabled;

  // Update indices strategies
  private final Collection<UpdateIndicesStrategy> updateStrategies;

  public static final Set<ChangeType> UPDATE_CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.UPSERT,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  public UpdateIndicesService(
      UpdateGraphIndicesService updateGraphIndicesService,
      SystemMetadataService systemMetadataService,
      @Nonnull Collection<UpdateIndicesStrategy> updateStrategies,
      boolean searchDiffMode,
      boolean structuredPropertiesHookEnabled,
      boolean structuredPropertiesWriteEnabled) {
    this.updateGraphIndicesService = updateGraphIndicesService;
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
    // Convert MetadataChangeLog events to MCLItems for batch processing
    List<MCLItem> mclItems =
        events.stream()
            .map(event -> MCLItemImpl.builder().build(event, opContext.getAspectRetriever()))
            .collect(Collectors.toList());

    // Apply side effects to generate additional MCLItems
    List<MCLItem> sideEffects =
        AspectsBatch.applyMCLSideEffects(mclItems, opContext.getRetrieverContext())
            .collect(Collectors.toList());

    // Group events by URN for batch processing while preserving order. Cross-routing-key mixing
    // is segregated upstream in the Kafka batch listener (see MCLBatchKafkaListener.consumeBatch),
    // so every call to this method already arrives under a single routing identity — grouping by
    // URN alone is sufficient here.
    LinkedHashMap<Urn, List<MCLItem>> groupedEvents =
        UpdateIndicesUtil.groupEventsByUrn(Stream.concat(mclItems.stream(), sideEffects.stream()));

    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        strategy.processBatch(opContext, groupedEvents, structuredPropertiesHookEnabled);
      }
    }

    // Process each group of events for the same URN. Each strategy manages its own
    // TimeseriesWriteThrottleCache/ThrottleSummary internally within processBatch above.

    for (List<MCLItem> urnEvents : groupedEvents.values()) {
      // Process update events
      List<MCLItem> updateEvents =
          urnEvents.stream()
              .filter(e -> UPDATE_CHANGE_TYPES.contains(e.getMetadataChangeLog().getChangeType()))
              .collect(Collectors.toList());

      if (!updateEvents.isEmpty()) {
        // Update graph indices for each event individually for now
        for (MCLItem event : updateEvents) {
          updateGraphIndicesService.handleChangeEvent(opContext, event.getMetadataChangeLog());
        }

        handleSystemMetadataUpdateChangeEvents(opContext, updateEvents);
      }

      // Process delete events
      List<MCLItem> deleteEvents =
          urnEvents.stream()
              .filter(e -> e.getMetadataChangeLog().getChangeType() == ChangeType.DELETE)
              .collect(Collectors.toList());

      for (MCLItem deleteEvent : deleteEvents) {
        Pair<EntitySpec, AspectSpec> specPair = UpdateIndicesUtil.extractSpecPair(deleteEvent);
        boolean isDeletingKey = UpdateIndicesUtil.isDeletingKey(specPair);

        // graph update first
        updateGraphIndicesService.handleChangeEvent(opContext, deleteEvent.getMetadataChangeLog());

        // system metadata is last for tracing
        handleSystemMetadataDeleteChangeEvent(
            opContext, deleteEvent.getUrn(), specPair, isDeletingKey);
      }
    }
  }

  /**
   * Handles system metadata updates for a collection of update change events. This method processes
   * system metadata separately for tracing purposes.
   *
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
              opContext,
              systemMetadata,
              event.getUrn().toString(),
              event.getAspectSpec().getName());

          // If processing status aspect update all aspects for this urn to removed
          if (event.getAspectSpec().getName().equals(Constants.STATUS_ASPECT_NAME)) {
            RecordTemplate aspect = event.getRecordTemplate();
            if (aspect instanceof Status) {
              systemMetadataService.setDocStatus(
                  opContext, event.getUrn().toString(), ((Status) aspect).isRemoved());
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
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      Pair<EntitySpec, AspectSpec> specPair,
      boolean isDeletingKey) {
    if (!specPair.getSecond().isTimeseries()) {
      if (isDeletingKey) {
        // Delete all aspects
        log.debug(String.format("Deleting all system metadata for urn: %s", urn));
        systemMetadataService.deleteUrn(opContext, urn.toString());
      } else {
        // Delete all aspects from system metadata service
        log.debug(
            String.format(
                "Deleting system metadata for urn: %s, aspect: %s",
                urn, specPair.getSecond().getName()));
        systemMetadataService.deleteAspect(
            opContext, urn.toString(), specPair.getSecond().getName());
      }
    }
  }

  /**
   * Updates index mappings for structured property changes. This method delegates to all enabled
   * strategies to ensure both V2 and V3 mappings are updated when needed.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param entitySpec the entity specification
   * @param aspectSpec the aspect specification
   * @param newValue the new aspect value
   * @param oldValue the old aspect value
   */
  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    for (UpdateIndicesStrategy strategy : updateStrategies) {
      if (strategy.isEnabled()) {
        strategy.updateIndexMappings(opContext, urn, entitySpec, aspectSpec, newValue, oldValue);
      }
    }
  }
}
