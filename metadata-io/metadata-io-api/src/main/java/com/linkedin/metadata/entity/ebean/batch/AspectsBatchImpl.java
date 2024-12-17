package com.linkedin.metadata.entity.ebean.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
@Builder(toBuilder = true)
public class AspectsBatchImpl implements AspectsBatch {

  @Nonnull private final Collection<? extends BatchItem> items;
  @Nonnull private final Collection<? extends BatchItem> nonRepeatedItems;
  @Getter @Nonnull private final RetrieverContext retrieverContext;

  @Override
  @Nonnull
  public Collection<? extends BatchItem> getItems() {
    return nonRepeatedItems;
  }

  @Override
  public Collection<? extends BatchItem> getInitialItems() {
    return items;
  }

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
      Map<String, Map<String, SystemAspect>> latestAspects,
      Map<String, Map<String, Long>> nextVersions) {

    // Process proposals to change items
    Stream<? extends BatchItem> mutatedProposalsStream =
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

                  return AspectsBatch.incrementBatchVersion(
                      upsertItem, latestAspects, nextVersions);
                })
            .collect(Collectors.toCollection(LinkedList::new));

    // Apply write hooks before side effects
    applyWriteMutationHooks(upsertBatchItems);

    LinkedList<ChangeMCP> newItems =
        applyMCPSideEffects(upsertBatchItems).collect(Collectors.toCollection(LinkedList::new));
    upsertBatchItems.addAll(newItems);

    Map<String, Set<String>> newUrnAspectNames =
        getNewUrnAspectsMap(getUrnAspectsMap(), upsertBatchItems);

    return Pair.of(newUrnAspectNames, upsertBatchItems);
  }

  private Stream<? extends BatchItem> proposedItemsToChangeItemStream(List<MCPItem> proposedItems) {
    List<MutationHook> mutationHooks =
        retrieverContext.getAspectRetriever().getEntityRegistry().getAllMutationHooks();
    Stream<? extends BatchItem> unmutatedItems =
        proposedItems.stream()
            .filter(
                proposedItem ->
                    mutationHooks.stream()
                        .noneMatch(
                            mutationHook ->
                                mutationHook.shouldApply(
                                    proposedItem.getChangeType(),
                                    proposedItem.getUrn(),
                                    proposedItem.getAspectName())))
            .map(mcpItem -> patchDiscriminator(mcpItem, retrieverContext.getAspectRetriever()));
    List<MCPItem> mutatedItems =
        applyProposalMutationHooks(proposedItems, retrieverContext).collect(Collectors.toList());
    Stream<? extends BatchItem> proposedItemsToChangeItems =
        mutatedItems.stream()
            .filter(mcpItem -> mcpItem.getMetadataChangeProposal() != null)
            // Filter on proposed items again to avoid applying builder to Patch Item side effects
            .filter(mcpItem -> mcpItem instanceof ProposedItem)
            .map(mcpItem -> patchDiscriminator(mcpItem, retrieverContext.getAspectRetriever()));
    Stream<? extends BatchItem> sideEffectItems =
        mutatedItems.stream().filter(mcpItem -> !(mcpItem instanceof ProposedItem));
    Stream<? extends BatchItem> combinedChangeItems =
        Stream.concat(proposedItemsToChangeItems, unmutatedItems);
    return Stream.concat(combinedChangeItems, sideEffectItems);
  }

  private static BatchItem patchDiscriminator(MCPItem mcpItem, AspectRetriever aspectRetriever) {
    if (ChangeType.PATCH.equals(mcpItem.getChangeType())) {
      return PatchItemImpl.PatchItemImplBuilder.build(
          mcpItem.getMetadataChangeProposal(),
          mcpItem.getAuditStamp(),
          aspectRetriever.getEntityRegistry());
    }
    return ChangeItemImpl.ChangeItemImplBuilder.build(
        mcpItem.getMetadataChangeProposal(), mcpItem.getAuditStamp(), aspectRetriever);
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
        Collection<MetadataChangeProposal> mcps,
        AuditStamp auditStamp,
        RetrieverContext retrieverContext) {
      return mcps(mcps, auditStamp, retrieverContext, false);
    }

    public AspectsBatchImplBuilder mcps(
        Collection<MetadataChangeProposal> mcps,
        AuditStamp auditStamp,
        RetrieverContext retrieverContext,
        boolean alternateMCPValidation) {

      retrieverContext(retrieverContext);
      items(
          mcps.stream()
              .map(
                  mcp -> {
                    try {
                      if (alternateMCPValidation) {
                        EntitySpec entitySpec =
                            retrieverContext
                                .getAspectRetriever()
                                .getEntityRegistry()
                                .getEntitySpec(mcp.getEntityType());
                        return ProposedItem.builder()
                            .metadataChangeProposal(mcp)
                            .entitySpec(entitySpec)
                            .auditStamp(auditStamp)
                            .build();
                      }
                      if (mcp.getChangeType().equals(ChangeType.PATCH)) {
                        return PatchItemImpl.PatchItemImplBuilder.build(
                            mcp,
                            auditStamp,
                            retrieverContext.getAspectRetriever().getEntityRegistry());
                      } else {
                        return ChangeItemImpl.ChangeItemImplBuilder.build(
                            mcp, auditStamp, retrieverContext.getAspectRetriever());
                      }
                    } catch (IllegalArgumentException e) {
                      log.error("Invalid proposal, skipping and proceeding with batch: {}", mcp, e);
                      return null;
                    }
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));
      return this;
    }

    private static <T extends BatchItem> List<T> filterRepeats(Collection<T> items) {
      List<T> result = new ArrayList<>();
      Map<Pair<Urn, String>, T> last = new HashMap<>();

      for (T item : items) {
        Pair<Urn, String> urnAspect = Pair.of(item.getUrn(), item.getAspectName());
        // Check if this item is a duplicate of the previous
        if (!last.containsKey(urnAspect) || !item.isDatabaseDuplicateOf(last.get(urnAspect))) {
          result.add(item);
        }
        last.put(urnAspect, item);
      }

      return result;
    }

    public AspectsBatchImpl build() {
      this.nonRepeatedItems = filterRepeats(this.items);

      ValidationExceptionCollection exceptions =
          AspectsBatch.validateProposed(this.nonRepeatedItems, this.retrieverContext);
      if (!exceptions.isEmpty()) {
        throw new IllegalArgumentException("Failed to validate MCP due to: " + exceptions);
      }

      return new AspectsBatchImpl(this.items, this.nonRepeatedItems, this.retrieverContext);
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
