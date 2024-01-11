package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.SystemAspect;
import com.linkedin.metadata.aspect.batch.UpsertItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class AspectsBatchImpl implements AspectsBatch {

  private final List<? extends BatchItem> items;

  /**
   * Convert patches to upserts, apply hooks at the aspect and batch level.
   *
   * @param latestAspects latest version in the database
   * @param entityRegistry entity registry
   * @return The new urn/aspectnames and the uniform upserts, possibly expanded/mutated by the
   *     various hooks
   */
  @Override
  public Pair<Map<String, Set<String>>, List<UpsertItem>> toUpsertBatchItems(
      final Map<String, Map<String, SystemAspect>> latestAspects,
      EntityRegistry entityRegistry,
      AspectRetriever aspectRetriever) {

    LinkedList<UpsertItem> upsertBatchItems =
        items.stream()
            .map(
                item -> {
                  final String urnStr = item.getUrn().toString();
                  // latest is also the old aspect
                  final SystemAspect latest =
                      latestAspects.getOrDefault(urnStr, Map.of()).get(item.getAspectName());

                  final MCPUpsertBatchItem upsertItem;
                  if (item instanceof MCPUpsertBatchItem) {
                    upsertItem = (MCPUpsertBatchItem) item;
                  } else {
                    // patch to upsert
                    MCPPatchBatchItem patchBatchItem = (MCPPatchBatchItem) item;
                    final RecordTemplate currentValue =
                        latest != null ? latest.getRecordTemplate(entityRegistry) : null;
                    upsertItem =
                        patchBatchItem.applyPatch(entityRegistry, currentValue, aspectRetriever);
                  }

                  // Apply hooks
                  final SystemMetadata oldSystemMetadata =
                      latest != null ? latest.getSystemMetadata() : null;
                  final RecordTemplate oldAspectValue =
                      latest != null ? latest.getRecordTemplate(entityRegistry) : null;
                  upsertItem.applyMutationHooks(
                      oldAspectValue, oldSystemMetadata, entityRegistry, aspectRetriever);

                  return upsertItem;
                })
            .collect(Collectors.toCollection(LinkedList::new));

    LinkedList<UpsertItem> newItems =
        applyMCPSideEffects(upsertBatchItems, entityRegistry, aspectRetriever)
            .collect(Collectors.toCollection(LinkedList::new));
    Map<String, Set<String>> newUrnAspectNames = getNewUrnAspectsMap(getUrnAspectsMap(), newItems);
    upsertBatchItems.addAll(newItems);

    return Pair.of(newUrnAspectNames, upsertBatchItems);
  }

  public static class AspectsBatchImplBuilder {
    /**
     * Just one aspect record template
     *
     * @param data aspect data
     * @return builder
     */
    public AspectsBatchImplBuilder one(BatchItem data) {
      this.items = List.of(data);
      return this;
    }

    public AspectsBatchImplBuilder mcps(
        List<MetadataChangeProposal> mcps,
        AuditStamp auditStamp,
        EntityRegistry entityRegistry,
        AspectRetriever aspectRetriever) {
      this.items =
          mcps.stream()
              .map(
                  mcp -> {
                    if (mcp.getChangeType().equals(ChangeType.PATCH)) {
                      return MCPPatchBatchItem.MCPPatchBatchItemBuilder.build(
                          mcp, auditStamp, entityRegistry);
                    } else {
                      return MCPUpsertBatchItem.MCPUpsertBatchItemBuilder.build(
                          mcp, auditStamp, entityRegistry, aspectRetriever);
                    }
                  })
              .collect(Collectors.toList());
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AspectsBatchImpl that = (AspectsBatchImpl) o;
    return Objects.equals(items, that.items);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }

  @Override
  public String toString() {
    return "AspectsBatchImpl{" + "items=" + items + '}';
  }
}
