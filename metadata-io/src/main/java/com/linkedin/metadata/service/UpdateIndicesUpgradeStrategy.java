package com.linkedin.metadata.service;

import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Rollback dual-write for V2 entity indices during an incremental reindex. After Phase 1 swaps an
 * alias to the next index, live writes are also applied to the OLD backing index so the alias can
 * be swapped back to it if the release is rolled back.
 *
 * <p>Targets come from {@link IncrementalReindexWriteTargets}, keyed by the V2 alias, so an event
 * resolves its target from the index its entity writes to. V3 indices take full documents with
 * external versions and are dual-written by {@link UpdateIndicesV3Strategy} from the same targets;
 * a V2 alias never resolves to a V3 target, so nothing here needs to tell the generations apart.
 *
 * <p>No-op while no V2 index has a swapped alias with rollback dual-write pending.
 */
@Slf4j
public class UpdateIndicesUpgradeStrategy implements UpdateIndicesStrategy {

  private final ElasticSearchService elasticSearchService;
  private final SearchDocumentTransformer searchDocumentTransformer;
  private final IncrementalReindexWriteTargets incrementalReindexWriteTargets;

  public UpdateIndicesUpgradeStrategy(
      @Nonnull final ElasticSearchService elasticSearchService,
      @Nonnull final SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull final IncrementalReindexWriteTargets incrementalReindexWriteTargets) {
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentTransformer = searchDocumentTransformer;
    this.incrementalReindexWriteTargets = incrementalReindexWriteTargets;
  }

  @Override
  public boolean isEnabled() {
    return incrementalReindexWriteTargets.hasRollbackTargets(EntityIndexVersion.V2);
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (!isEnabled()) {
      return;
    }

    for (List<MCLItem> urnEvents : groupedEvents.values()) {
      for (MCLItem event : urnEvents) {
        ChangeType changeType = event.getMetadataChangeLog().getChangeType();
        if (UPDATE_CHANGE_TYPES.contains(changeType)) {
          processUpdateEvent(opContext, event);
        } else if (changeType == ChangeType.DELETE) {
          processDeleteEvent(opContext, event);
        }
      }
    }
  }

  private void processUpdateEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    final String indexName = v2IndexName(opContext, event);
    final Optional<String> oldIndex = incrementalReindexWriteTargets.oldBackingIndexFor(indexName);
    if (oldIndex.isEmpty()) {
      return;
    }

    try {
      Optional<ObjectNode> searchDocument =
          searchDocumentTransformer.transformAspect(
              opContext,
              event.getUrn(),
              event.getRecordTemplate(),
              event.getAspectSpec(),
              false,
              event.getAuditStamp());

      if (searchDocument.isEmpty()) {
        return;
      }

      String docId =
          opContext.getSearchContext().getIndexConvention().getEntityDocumentId(event.getUrn());
      String document = searchDocument.get().toString();

      elasticSearchService.upsertDocumentByIndexName(opContext, oldIndex.get(), document, docId);

      incrementalReindexWriteTargets.recordDualWriteStarted(indexName);

      log.debug(
          "Rollback dual-write: upserted doc to '{}' for index '{}', urn '{}'",
          oldIndex.get(),
          indexName,
          event.getUrn());
    } catch (Exception e) {
      log.error(
          "Rollback dual-write failed for index '{}', urn '{}', old index '{}': {}",
          indexName,
          event.getUrn(),
          oldIndex.get(),
          e.getMessage(),
          e);
    }
  }

  private void processDeleteEvent(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    final String indexName = v2IndexName(opContext, event);
    final Optional<String> oldIndex = incrementalReindexWriteTargets.oldBackingIndexFor(indexName);
    if (oldIndex.isEmpty()) {
      return;
    }

    boolean isDeletingKey =
        event.getEntitySpec().getKeyAspectSpec().getName().equals(event.getAspectName());
    if (!isDeletingKey) {
      processUpdateEvent(opContext, event);
      return;
    }

    try {
      String docId =
          opContext.getSearchContext().getIndexConvention().getEntityDocumentId(event.getUrn());
      elasticSearchService.deleteDocumentByIndexName(opContext, oldIndex.get(), docId);

      log.debug(
          "Rollback dual-write: deleted doc from '{}' for index '{}', urn '{}'",
          oldIndex.get(),
          indexName,
          event.getUrn());
    } catch (Exception e) {
      log.error(
          "Rollback dual-write delete failed for index '{}', urn '{}': {}",
          indexName,
          event.getUrn(),
          e.getMessage(),
          e);
    }
  }

  private static String v2IndexName(@Nonnull OperationContext opContext, @Nonnull MCLItem event) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .getEntityIndexName(opContext, event.getEntitySpec().getName());
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext) {
    return Collections.emptyList();
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return Collections.emptyList();
  }

  @Override
  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    // No-op: next indices were created with target mappings during Phase 1
  }
}
