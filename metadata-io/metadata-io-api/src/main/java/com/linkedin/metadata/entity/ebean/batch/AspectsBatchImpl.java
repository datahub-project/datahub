package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class AspectsBatchImpl implements AspectsBatch {

  @Nonnull private final Collection<? extends BatchItem> items;
  @Nonnull private final RetrieverContext retrieverContext;

  /**
   * Convert patches to upserts, apply hooks at the aspect and batch level.
   *
   * <p>Filter CREATE if not exists
   *
   * @param latestAspects latest version in the database
   * @return The new urn/aspectnames and the uniform upserts, possibly expanded/mutated by the
   *     various hooks
   */
  @Override
  public Pair<Map<String, Set<String>>, List<ChangeMCP>> toUpsertBatchItems(
      final Map<String, Map<String, SystemAspect>> latestAspects) {

    // Process proposals to change items
    Stream<ChangeMCP> mutatedProposalsStream =
        proposedItemsToChangeItemStream(
            items.stream()
                .filter(item -> item instanceof ProposedItem)
                .map(item -> (MCPItem) item)
                .collect(Collectors.toList()));
    // Regular change items
    Stream<? extends BatchItem> changeMCPStream =
        items.stream().filter(item -> !(item instanceof ProposedItem));

    // Convert patches to upserts if needed
    LinkedList<ChangeMCP> upsertBatchItems =
        Stream.concat(mutatedProposalsStream, changeMCPStream)
            .map(
                item -> {
                  final String urnStr = item.getUrn().toString();
                  // latest is also the old aspect
                  final SystemAspect latest =
                      latestAspects.getOrDefault(urnStr, Map.of()).get(item.getAspectName());

                  final ChangeItemImpl upsertItem;
                  if (item instanceof ChangeItemImpl) {
                    upsertItem = (ChangeItemImpl) item;
                  } else {
                    // patch to upsert
                    PatchItemImpl patchBatchItem = (PatchItemImpl) item;
                    final RecordTemplate currentValue =
                        latest != null ? latest.getRecordTemplate() : null;
                    upsertItem =
                        patchBatchItem.applyPatch(
                            currentValue, retrieverContext.getAspectRetriever());
                  }

                  // Populate old aspect for write hooks
                  upsertItem.setPreviousSystemAspect(latest);

                  return upsertItem;
                })
            .collect(Collectors.toCollection(LinkedList::new));

    // Apply write hooks before side effects
    applyWriteMutationHooks(upsertBatchItems);

    LinkedList<ChangeMCP> newItems =
        applyMCPSideEffects(upsertBatchItems).collect(Collectors.toCollection(LinkedList::new));
    Map<String, Set<String>> newUrnAspectNames = getNewUrnAspectsMap(getUrnAspectsMap(), newItems);
    upsertBatchItems.addAll(newItems);

    return Pair.of(newUrnAspectNames, upsertBatchItems);
  }

  private Stream<ChangeMCP> proposedItemsToChangeItemStream(List<MCPItem> proposedItems) {
    return applyProposalMutationHooks(proposedItems, retrieverContext)
        .filter(mcpItem -> mcpItem.getMetadataChangeProposal() != null)
        .map(
            mcpItem ->
                ChangeItemImpl.ChangeItemImplBuilder.build(
                    mcpItem.getMetadataChangeProposal(),
                    mcpItem.getAuditStamp(),
                    retrieverContext.getAspectRetriever()));
  }

  public static class AspectsBatchImplBuilder {
    /**
     * Just one aspect record template
     *
     * @param data aspect data
     * @return builder
     */
    public AspectsBatchImplBuilder one(BatchItem data, RetrieverContext retrieverContext) {
      retrieverContext(retrieverContext);
      items(List.of(data));
      return this;
    }

    public AspectsBatchImplBuilder mcps(
        List<MetadataChangeProposal> mcps,
        AuditStamp auditStamp,
        RetrieverContext retrieverContext) {

      retrieverContext(retrieverContext);
      items(
          mcps.stream()
              .map(
                  mcp -> {
                    if (mcp.getChangeType().equals(ChangeType.PATCH)) {
                      return PatchItemImpl.PatchItemImplBuilder.build(
                          mcp,
                          auditStamp,
                          retrieverContext.getAspectRetriever().getEntityRegistry());
                    } else {
                      return ChangeItemImpl.ChangeItemImplBuilder.build(
                          mcp, auditStamp, retrieverContext.getAspectRetriever());
                    }
                  })
              .collect(Collectors.toList()));
      return this;
    }

    public AspectsBatchImpl build() {
      ValidationExceptionCollection exceptions =
          AspectsBatch.validateProposed(this.items, this.retrieverContext);
      if (!exceptions.isEmpty()) {
        throw new IllegalArgumentException("Failed to validate MCP due to: " + exceptions);
      }

      return new AspectsBatchImpl(this.items, this.retrieverContext);
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
