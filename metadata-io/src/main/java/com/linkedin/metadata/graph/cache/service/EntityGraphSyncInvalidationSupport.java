package com.linkedin.metadata.graph.cache.service;

import static com.linkedin.metadata.Constants.SYNC_INDEX_UPDATE_HEADER_NAME;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationEntry;
import com.linkedin.metadata.utils.SyncSearchIndexUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds {@link SyncGraphInvalidationBatch} from sync metadata writes.
 *
 * <p><b>Sync gate:</b> batches are empty unless the write requires a synchronous Elasticsearch
 * index update ({@link SyncSearchIndexUtils#requiresSyncSearchIndexUpdate}) or carries the
 * sync-index header — the same conditions as inline {@code UpdateIndicesService} indexing in {@code
 * EntityServiceImpl.preprocessEvent}.
 *
 * <p><b>Entity delete vs aspect delete:</b> graph registry indexes invalidation candidates by
 * <em>relationship aspect</em> (e.g. {@code domainProperties}), not key aspects. Key-aspect or hard
 * deletes set {@link SyncGraphInvalidationEntry#getAspectName()} to {@code null} for entity-wide
 * lookup via {@link com.linkedin.metadata.graph.cache.EntityGraphCache#getGraphIdsForEntityType}.
 * Relationship-aspect deletes use {@code getCandidateGraphIds}. Deletes are omitted when the
 * registry has no matching graph configuration.
 *
 * <p><b>Create detection:</b> an ingest batch item is treated as a create when its URN also has a
 * key-aspect item in the same batch (or {@code UpdateAspectResult} shows first write of a non-key
 * aspect on a new entity).
 *
 * <p><b>No-op writes:</b> invalidation is skipped when primary storage stamped {@code
 * systemMetadata.properties.isNoOp=true} ({@link SystemMetadataUtils#isNoOp}) on the ingest result
 * or MCL. {@link ChangeType#RESTATE} alone is not used — it can still drive search/graph indexing
 * without a primary-storage change.
 */
public final class EntityGraphSyncInvalidationSupport {

  /** ASCII Record Separator — cannot appear in URNs. */
  private static final char COMPOSITE_KEY_SEPARATOR = '\u001e';

  private EntityGraphSyncInvalidationSupport() {}

  @Nonnull
  static String compositeKey(@Nonnull String urn, @Nonnull String aspectName) {
    return urn + COMPOSITE_KEY_SEPARATOR + aspectName;
  }

  @Nonnull
  public static SyncGraphInvalidationBatch fromSyncIngestBatch(
      @Nonnull OperationContext opContext,
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull AspectsBatch aspectsBatch,
      @Nonnull List<UpdateAspectResult> updateResults) {
    return buildIngestBatch(opContext, preProcessHooks, aspectsBatch, updateResults);
  }

  @Nonnull
  private static SyncGraphInvalidationBatch buildIngestBatch(
      @Nonnull OperationContext opContext,
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull AspectsBatch aspectsBatch,
      @Nonnull List<UpdateAspectResult> updateResults) {
    List<? extends BatchItem> batchItems = new ArrayList<>(aspectsBatch.getItems());
    Set<String> urnsWithKeyAspect = new LinkedHashSet<>();
    List<UpdateAspectResult> materialResults = new ArrayList<>();
    for (int i = 0; i < updateResults.size(); i++) {
      UpdateAspectResult result = updateResults.get(i);
      if (SystemMetadataUtils.isNoOp(result.getNewSystemMetadata())) {
        continue;
      }
      BatchItem batchItem = i < batchItems.size() ? batchItems.get(i) : null;
      if (batchItem == null || !isSyncItem(opContext, preProcessHooks, batchItem)) {
        continue;
      }
      materialResults.add(result);
      String aspectName = result.getRequest().getAspectName();
      if (isKeyAspect(opContext, result.getUrn(), aspectName)) {
        urnsWithKeyAspect.add(result.getUrn().toString());
      }
    }

    if (materialResults.isEmpty()) {
      return SyncGraphInvalidationBatch.empty();
    }

    var builder = SyncGraphInvalidationBatch.builder();
    Set<String> seenCreates = new HashSet<>();
    Set<String> seenDeletes = new HashSet<>();
    Set<String> seenUpdates = new HashSet<>();

    for (UpdateAspectResult result : materialResults) {
      String urn = result.getUrn().toString();
      String entityType = result.getUrn().getEntityType();
      String aspectName = result.getRequest().getAspectName();
      ChangeType changeType = result.getRequest().getChangeType();

      if (changeType == ChangeType.DELETE) {
        boolean entireEntity = isKeyAspect(opContext, result.getUrn(), aspectName);
        if (!deleteAffectsConfiguredGraph(opContext, entityType, aspectName, entireEntity)) {
          continue;
        }
        String deleteKey = entireEntity ? urn : compositeKey(urn, aspectName);
        if (seenDeletes.add(deleteKey)) {
          builder.delete(
              SyncGraphInvalidationEntry.builder()
                  .entityUrn(urn)
                  .entityType(entityType)
                  .aspectName(entireEntity ? null : aspectName)
                  .build());
        }
        continue;
      }

      if (urnsWithKeyAspect.contains(urn)) {
        String createKey = compositeKey(urn, aspectName);
        if (seenCreates.add(createKey)) {
          builder.create(
              SyncGraphInvalidationEntry.builder()
                  .entityUrn(urn)
                  .entityType(entityType)
                  .aspectName(aspectName)
                  .build());
        }
        continue;
      }

      if (result.getOldValue() == null
          && result.getNewValue() != null
          && aspectName != null
          && !opContext
              .getEntityGraphCache()
              .getCandidateGraphIds(entityType, aspectName)
              .isEmpty()) {
        String createKey = compositeKey(urn, aspectName);
        if (seenCreates.add(createKey)) {
          builder.create(
              SyncGraphInvalidationEntry.builder()
                  .entityUrn(urn)
                  .entityType(entityType)
                  .aspectName(aspectName)
                  .build());
        }
        continue;
      }

      String updateKey = compositeKey(urn, aspectName);
      if (seenUpdates.add(updateKey)) {
        builder.update(
            SyncGraphInvalidationEntry.builder()
                .entityUrn(urn)
                .entityType(entityType)
                .aspectName(aspectName)
                .build());
      }
    }

    return builder.build();
  }

  @Nonnull
  public static SyncGraphInvalidationBatch fromSyncMetadataChangeLog(
      @Nonnull OperationContext opContext,
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull MetadataChangeLog metadataChangeLog) {
    if (!isSyncMetadataChangeLog(opContext, preProcessHooks, metadataChangeLog)) {
      return SyncGraphInvalidationBatch.empty();
    }
    if (SystemMetadataUtils.isNoOp(metadataChangeLog.getSystemMetadata())) {
      return SyncGraphInvalidationBatch.empty();
    }
    String urn = metadataChangeLog.getEntityUrn().toString();
    String entityType = metadataChangeLog.getEntityType();
    String aspectName = metadataChangeLog.getAspectName();
    ChangeType changeType = metadataChangeLog.getChangeType();
    var builder = SyncGraphInvalidationBatch.builder();
    SyncGraphInvalidationEntry entry =
        SyncGraphInvalidationEntry.builder()
            .entityUrn(urn)
            .entityType(entityType)
            .aspectName(aspectName)
            .build();

    if (changeType == ChangeType.DELETE) {
      boolean entireEntity = isKeyAspect(opContext, metadataChangeLog.getEntityUrn(), aspectName);
      if (!deleteAffectsConfiguredGraph(opContext, entityType, aspectName, entireEntity)) {
        return SyncGraphInvalidationBatch.empty();
      }
      builder.delete(
          SyncGraphInvalidationEntry.builder()
              .entityUrn(urn)
              .entityType(entityType)
              .aspectName(entireEntity ? null : aspectName)
              .build());
      return builder.build();
    }

    if (isKeyAspect(opContext, metadataChangeLog.getEntityUrn(), aspectName)
        || changeType == ChangeType.CREATE_ENTITY
        || changeType == ChangeType.CREATE) {
      builder.create(entry);
    } else {
      builder.update(entry);
    }
    return builder.build();
  }

  /**
   * Builds a delete batch from {@code EntityServiceImpl.deleteAspectWithoutMCL} (including {@code
   * deleteUrn}).
   *
   * @param entireEntity {@code true} when the key aspect was removed or hard delete cleared the
   *     entity — sets {@code aspectName} to {@code null} for entity-wide graph lookup
   */
  @Nonnull
  public static SyncGraphInvalidationBatch fromSyncEntityDelete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityUrn,
      @Nonnull String entityType,
      @Nullable String aspectName,
      boolean entireEntity) {
    if (!deleteAffectsConfiguredGraph(opContext, entityType, aspectName, entireEntity)) {
      return SyncGraphInvalidationBatch.empty();
    }
    return SyncGraphInvalidationBatch.builder()
        .delete(
            SyncGraphInvalidationEntry.builder()
                .entityUrn(entityUrn)
                .entityType(entityType)
                .aspectName(entireEntity ? null : aspectName)
                .build())
        .build();
  }

  /**
   * Builds an update batch from relationship-aspect version rollback in {@code
   * EntityServiceImpl.deleteAspectWithoutMCL} when a surviving older aspect remains ({@code
   * ChangeType.UPSERT}).
   */
  @Nonnull
  public static SyncGraphInvalidationBatch fromSyncAspectRollback(
      @Nonnull OperationContext opContext,
      @Nonnull String entityUrn,
      @Nonnull String entityType,
      @Nullable String aspectName) {
    if (aspectName == null || !updateAffectsConfiguredGraph(opContext, entityType, aspectName)) {
      return SyncGraphInvalidationBatch.empty();
    }
    return SyncGraphInvalidationBatch.builder()
        .update(
            SyncGraphInvalidationEntry.builder()
                .entityUrn(entityUrn)
                .entityType(entityType)
                .aspectName(aspectName)
                .build())
        .build();
  }

  private static boolean isSyncItem(
      @Nonnull OperationContext opContext,
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull BatchItem item) {
    if (isSyncIndexHeader(item)) {
      return true;
    }
    return SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
        preProcessHooks, opContext, item.getSystemMetadata());
  }

  private static boolean isSyncMetadataChangeLog(
      @Nonnull OperationContext opContext,
      @Nonnull PreProcessHooks preProcessHooks,
      @Nonnull MetadataChangeLog metadataChangeLog) {
    if (metadataChangeLog.getHeaders() != null
        && Boolean.parseBoolean(
            metadataChangeLog.getHeaders().getOrDefault(SYNC_INDEX_UPDATE_HEADER_NAME, "false"))) {
      return true;
    }
    return SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
        preProcessHooks, opContext, metadataChangeLog.getSystemMetadata());
  }

  private static boolean isSyncIndexHeader(@Nonnull BatchItem item) {
    if (!(item instanceof MCPItem mcpItem)) {
      return false;
    }
    return Boolean.parseBoolean(
        mcpItem.getHeaders().getOrDefault(SYNC_INDEX_UPDATE_HEADER_NAME, "false"));
  }

  /**
   * Whether sync delete invalidation should be considered for this write. Relationship-aspect
   * deletes use {@link com.linkedin.metadata.graph.cache.EntityGraphCache#getCandidateGraphIds};
   * entity-wide deletes use {@link
   * com.linkedin.metadata.graph.cache.EntityGraphCache#getGraphIdsForEntityType} (resolved-edge
   * scan via {@link
   * com.linkedin.metadata.graph.cache.config.EntityGraphRegistry#getGraphIdsForEntityType}).
   */
  private static boolean deleteAffectsConfiguredGraph(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nullable String aspectName,
      boolean entireEntity) {
    if (entireEntity) {
      return !opContext.getEntityGraphCache().getGraphIdsForEntityType(entityType).isEmpty();
    }
    if (aspectName == null) {
      return false;
    }
    return !opContext.getEntityGraphCache().getCandidateGraphIds(entityType, aspectName).isEmpty();
  }

  private static boolean updateAffectsConfiguredGraph(
      @Nonnull OperationContext opContext, @Nonnull String entityType, @Nonnull String aspectName) {
    return !opContext.getEntityGraphCache().getCandidateGraphIds(entityType, aspectName).isEmpty();
  }

  private static boolean isKeyAspect(
      @Nonnull OperationContext opContext,
      @Nonnull com.linkedin.common.urn.Urn urn,
      String aspectName) {
    return aspectName != null && aspectName.equals(opContext.getKeyAspectName(urn));
  }
}
