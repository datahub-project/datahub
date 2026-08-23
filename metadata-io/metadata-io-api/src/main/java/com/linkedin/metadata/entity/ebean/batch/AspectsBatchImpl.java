package com.linkedin.metadata.entity.ebean.batch;

import com.datahub.context.OperationFingerprint;
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
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder(toBuilder = true)
public class AspectsBatchImpl implements AspectsBatch {

  @Nonnull private final Collection<? extends BatchItem> items;
  @Nonnull private final Collection<? extends BatchItem> nonRepeatedItems;
  @Getter @Nonnull private final RetrieverContext retrieverContext;

  /**
   * MCPs that failed item construction during {@link AspectsBatchImplBuilder#mcps} and were
   * soft-skipped so siblings could proceed. Empty when every input built successfully. On the
   * consumer path, EntityService emits FMCP for each; API accept does not.
   */
  @Getter @Builder.Default @Nonnull
  private final List<ConstructionFailure> constructionFailures = List.of();

  /** One MCP that failed to become a {@link BatchItem} during construction (soft-skipped). */
  @Value
  public static class ConstructionFailure {
    @Nonnull MetadataChangeProposal mcp;
    @Nonnull Throwable cause;
  }

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
      @Nonnull OperationFingerprint operationContext,
      Map<String, Map<String, SystemAspect>> latestAspects,
      Map<String, Map<String, Long>> nextVersions,
      BiFunction<ChangeMCP, SystemAspect, SystemAspect> databaseUpsert) {

    // Process proposals to change items
    Stream<? extends BatchItem> mutatedProposalsStream =
        proposedItemsToChangeItemStream(
            operationContext,
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
                      upsertItem, latestAspects, nextVersions, databaseUpsert);
                })
            .collect(Collectors.toCollection(LinkedList::new));

    // Apply write hooks before side effects
    applyWriteMutationHooks(operationContext, upsertBatchItems);

    LinkedList<ChangeMCP> newItems =
        applyMCPSideEffects(operationContext, upsertBatchItems)
            .collect(Collectors.toCollection(LinkedList::new));
    upsertBatchItems.addAll(newItems);

    Map<String, Set<String>> newUrnAspectNames =
        getNewUrnAspectsMap(getUrnAspectsMap(), upsertBatchItems);

    return Pair.of(newUrnAspectNames, upsertBatchItems);
  }

  private Stream<? extends BatchItem> proposedItemsToChangeItemStream(
      OperationFingerprint operationFingerprint, List<MCPItem> proposedItems) {
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
        applyProposalMutationHooks(operationFingerprint, proposedItems, retrieverContext)
            .collect(Collectors.toList());
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
      return PatchItemImpl.builder()
          .build(
              mcpItem.getMetadataChangeProposal(),
              mcpItem.getAuditStamp(),
              aspectRetriever.getEntityRegistry());
    }
    return ChangeItemImpl.builder()
        .build(mcpItem.getMetadataChangeProposal(), mcpItem.getAuditStamp(), aspectRetriever);
  }

  public static class AspectsBatchImplBuilder {

    /**
     * Per-MCP construction failures recorded during {@link #mcps}. Soft-skip siblings when some
     * items remain; throw when the batch would be empty (#19086). Cleared at the start of each
     * {@code mcps(...)} call.
     */
    private final List<ConstructionFailure> pendingConstructionFailures = new ArrayList<>();

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

      pendingConstructionFailures.clear();
      retrieverContext(retrieverContext);
      List<BatchItem> builtItems = new ArrayList<>();
      for (MetadataChangeProposal mcp : mcps) {
        try {
          final BatchItem item;
          if (alternateMCPValidation) {
            item =
                ProposedItem.builder()
                    .build(
                        mcp,
                        auditStamp,
                        retrieverContext.getAspectRetriever().getEntityRegistry());
          } else if (ChangeType.PATCH.equals(mcp.getChangeType())) {
            item =
                PatchItemImpl.builder()
                    .build(
                        mcp,
                        auditStamp,
                        retrieverContext.getAspectRetriever().getEntityRegistry());
          } else {
            item =
                ChangeItemImpl.builder()
                    .build(mcp, auditStamp, retrieverContext.getAspectRetriever());
          }
          builtItems.add(item);
        } catch (RuntimeException e) {
          // Normalize construction failures regardless of exception type (IAE vs
          // ValidationException vs UnsupportedOperationException). Soft-skip this MCP
          // and decide empty-batch fail vs proceed in build(opContext).
          pendingConstructionFailures.add(new ConstructionFailure(mcp, e));
          log.error(
              "Invalid proposal during construction, skipping item: entityUrn={},"
                  + " aspectName={}, changeType={}, reason={}",
              mcp.hasEntityUrn() ? mcp.getEntityUrn() : null,
              mcp.hasAspectName() ? mcp.getAspectName() : null,
              mcp.hasChangeType() ? mcp.getChangeType() : null,
              e.getMessage(),
              e);
        }
      }
      items(builtItems);
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

    public AspectsBatchImpl build(@Nullable OperationContext operationContext) {
      if (this.items == null) {
        this.items = Collections.emptyList();
      }

      // Empty after construction failures: silent success was #19086. Fail regardless of API vs
      // consumer — consumers FMCP via their outer catch / EntityService.
      if (!pendingConstructionFailures.isEmpty() && this.items.isEmpty()) {
        if (pendingConstructionFailures.size() == 1) {
          Throwable cause = pendingConstructionFailures.get(0).getCause();
          if (cause instanceof ValidationException) {
            throw (ValidationException) cause;
          }
          throw new ValidationException(
              "Invalid MetadataChangeProposal: " + cause.getMessage(), cause);
        }
        Throwable firstCause = pendingConstructionFailures.get(0).getCause();
        String message =
            pendingConstructionFailures.stream()
                .map(
                    failure -> "Invalid MetadataChangeProposal: " + failure.getCause().getMessage())
                .collect(Collectors.joining("; "));
        throw new ValidationException(message, firstCause);
      }

      this.nonRepeatedItems = filterRepeats(this.items);

      // operationContext serves dual roles here: OperationFingerprint for routing (1st arg)
      // and AuthorizationSession for per-user auth checks (4th arg). OperationContext
      // implements both interfaces; this matches pre-refactor behaviour.
      ValidationExceptionCollection exceptions =
          AspectsBatch.validateProposed(
              operationContext, this.nonRepeatedItems, this.retrieverContext, operationContext);
      if (!exceptions.isEmpty()) {
        throw new ValidationException(exceptions);
      }

      return new AspectsBatchImpl(
          this.items,
          this.nonRepeatedItems,
          this.retrieverContext,
          List.copyOf(this.pendingConstructionFailures));
    }

    private AspectsBatchImpl build() {
      return null;
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
