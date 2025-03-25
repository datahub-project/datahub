package com.linkedin.metadata.aspect.batch;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/**
 * A batch of aspects in the context of either an MCP or MCL write path to a data store. The item is
 * a record that encapsulates the change type, raw aspect and ancillary information like {@link
 * SystemMetadata} and record/message created time
 */
public interface AspectsBatch {
  Collection<? extends BatchItem> getItems();

  Collection<? extends BatchItem> getInitialItems();

  RetrieverContext getRetrieverContext();

  /**
   * Returns MCP items. Could be one of patch, upsert, etc.
   *
   * @return batch items
   */
  default List<MCPItem> getMCPItems() {
    return getItems().stream()
        .filter(item -> item instanceof MCPItem)
        .map(item -> (MCPItem) item)
        .collect(Collectors.toList());
  }

  /**
   * Convert patches to upserts, apply hooks at the aspect and batch level.
   *
   * @param latestAspects latest aspect in the database
   * @param nextVersions next version for the aspect
   * @param databaseUpsert function which upserts a given change MCP
   * @return The new urn/aspectnames and the uniform upserts, possibly expanded/mutated by the
   *     various hooks
   */
  Pair<Map<String, Set<String>>, List<ChangeMCP>> toUpsertBatchItems(
      Map<String, Map<String, SystemAspect>> latestAspects,
      Map<String, Map<String, Long>> nextVersions,
      BiFunction<ChangeMCP, SystemAspect, SystemAspect> databaseUpsert);

  /**
   * Apply read mutations to batch
   *
   * @param items
   */
  default void applyReadMutationHooks(Collection<ReadItem> items) {
    applyReadMutationHooks(items, getRetrieverContext());
  }

  static void applyReadMutationHooks(
      Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    for (MutationHook mutationHook :
        retrieverContext.getAspectRetriever().getEntityRegistry().getAllMutationHooks()) {
      mutationHook.applyReadMutation(items, retrieverContext);
    }
  }

  /**
   * Apply write mutations to batch
   *
   * @param changeMCPS
   */
  default void applyWriteMutationHooks(Collection<ChangeMCP> changeMCPS) {
    applyWriteMutationHooks(changeMCPS, getRetrieverContext());
  }

  static void applyWriteMutationHooks(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    for (MutationHook mutationHook :
        retrieverContext.getAspectRetriever().getEntityRegistry().getAllMutationHooks()) {
      mutationHook.applyWriteMutation(changeMCPS, retrieverContext);
    }
  }

  default Stream<MCPItem> applyProposalMutationHooks(
      Collection<MCPItem> proposedItems, @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext.getAspectRetriever().getEntityRegistry().getAllMutationHooks().stream()
        .flatMap(
            mutationHook -> mutationHook.applyProposalMutation(proposedItems, retrieverContext));
  }

  default <T extends BatchItem> ValidationExceptionCollection validateProposed(
      Collection<T> mcpItems) {
    return validateProposed(mcpItems, getRetrieverContext());
  }

  static <T extends BatchItem> ValidationExceptionCollection validateProposed(
      Collection<T> mcpItems, @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    retrieverContext
        .getAspectRetriever()
        .getEntityRegistry()
        .getAllAspectPayloadValidators()
        .stream()
        .flatMap(validator -> validator.validateProposed(mcpItems, retrieverContext))
        .forEach(exceptions::addException);
    return exceptions;
  }

  default ValidationExceptionCollection validatePreCommit(Collection<ChangeMCP> changeMCPs) {
    return validatePreCommit(changeMCPs, getRetrieverContext());
  }

  static ValidationExceptionCollection validatePreCommit(
      Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    retrieverContext
        .getAspectRetriever()
        .getEntityRegistry()
        .getAllAspectPayloadValidators()
        .stream()
        .flatMap(validator -> validator.validatePreCommit(changeMCPs, retrieverContext))
        .forEach(exceptions::addException);
    return exceptions;
  }

  default Stream<ChangeMCP> applyMCPSideEffects(Collection<ChangeMCP> items) {
    return applyMCPSideEffects(items, getRetrieverContext());
  }

  static Stream<ChangeMCP> applyMCPSideEffects(
      Collection<ChangeMCP> items, @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext.getAspectRetriever().getEntityRegistry().getAllMCPSideEffects().stream()
        .flatMap(mcpSideEffect -> mcpSideEffect.apply(items, retrieverContext));
  }

  default Stream<MCPItem> applyPostMCPSideEffects(Collection<MCLItem> items) {
    return applyPostMCPSideEffects(items, getRetrieverContext());
  }

  static Stream<MCPItem> applyPostMCPSideEffects(
      Collection<MCLItem> items, @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext.getAspectRetriever().getEntityRegistry().getAllMCPSideEffects().stream()
        .flatMap(mcpSideEffect -> mcpSideEffect.postApply(items, retrieverContext));
  }

  default Stream<MCLItem> applyMCLSideEffects(Collection<MCLItem> items) {
    return applyMCLSideEffects(items, getRetrieverContext());
  }

  static Stream<MCLItem> applyMCLSideEffects(
      Collection<MCLItem> items, @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext.getAspectRetriever().getEntityRegistry().getAllMCLSideEffects().stream()
        .flatMap(mclSideEffect -> mclSideEffect.apply(items, retrieverContext));
  }

  default boolean containsDuplicateAspects() {
    return getInitialItems().stream()
            .map(i -> String.format("%s_%s", i.getClass().getSimpleName(), i.hashCode()))
            .distinct()
            .count()
        != getItems().size();
  }

  default Map<String, List<? extends BatchItem>> duplicateAspects() {
    return getInitialItems().stream()
        .collect(
            Collectors.groupingBy(
                i -> String.format("%s_%s", i.getClass().getSimpleName(), i.hashCode())))
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null && entry.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  default Map<String, Set<String>> getUrnAspectsMap() {
    return getItems().stream()
        .map(aspect -> Pair.of(aspect.getUrn().toString(), aspect.getAspectName()))
        .collect(
            Collectors.groupingBy(
                Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
  }

  default Map<String, Set<String>> getNewUrnAspectsMap(
      Map<String, Set<String>> existingMap, List<? extends BatchItem> items) {
    Map<String, HashSet<String>> newItemsMap =
        items.stream()
            .map(aspect -> Pair.of(aspect.getUrn().toString(), aspect.getAspectName()))
            .collect(
                Collectors.groupingBy(
                    Pair::getKey,
                    Collectors.mapping(Pair::getValue, Collectors.toCollection(HashSet::new))));

    return newItemsMap.entrySet().stream()
        .filter(
            entry ->
                !existingMap.containsKey(entry.getKey())
                    || !existingMap.get(entry.getKey()).containsAll(entry.getValue()))
        .peek(
            entry -> {
              if (existingMap.containsKey(entry.getKey())) {
                entry.getValue().removeAll(existingMap.get(entry.getKey()));
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static <T> Map<String, Map<String, T>> merge(
      @Nonnull Map<String, Map<String, T>> a, @Nonnull Map<String, Map<String, T>> b) {

    Map<String, Map<String, T>> mergedMap = new HashMap<>();
    for (Map.Entry<String, Map<String, T>> entry :
        Stream.concat(a.entrySet().stream(), b.entrySet().stream()).collect(Collectors.toList())) {
      mergedMap.computeIfAbsent(entry.getKey(), k -> new HashMap<>()).putAll(entry.getValue());
    }
    return mergedMap;
  }

  default String toAbbreviatedString(int maxWidth) {
    return toAbbreviatedString(getItems(), maxWidth);
  }

  static String toAbbreviatedString(Collection<? extends BatchItem> items, int maxWidth) {
    List<String> itemsAbbreviated = new ArrayList<String>();
    items.forEach(
        item -> {
          if (item instanceof ChangeMCP) {
            itemsAbbreviated.add(((ChangeMCP) item).toAbbreviatedString());
          } else {
            itemsAbbreviated.add(item.toString());
          }
        });
    return "AspectsBatchImpl{"
        + "items="
        + StringUtils.abbreviate(itemsAbbreviated.toString(), maxWidth)
        + '}';
  }

  /**
   * Increment aspect within a batch, tracking both the next aspect version and the most recent
   *
   * @param changeMCP changeMCP to be incremented
   * @param latestAspects latest aspects within the batch
   * @param nextVersions next version for the aspects in the batch
   * @return the incremented changeMCP
   */
  static <T extends SystemAspect> ChangeMCP incrementBatchVersion(
      ChangeMCP changeMCP,
      Map<String, Map<String, T>> latestAspects,
      Map<String, Map<String, Long>> nextVersions,
      BiFunction<ChangeMCP, T, T> databaseUpsert) {

    // This is the current version of row 0
    // or in other words the next insertion row when the version 0 is rotated
    long nextVersion =
        nextVersions
            .getOrDefault(changeMCP.getUrn().toString(), Collections.emptyMap())
            .getOrDefault(changeMCP.getAspectName(), ASPECT_LATEST_VERSION);

    T currentSystemAspect =
        latestAspects
            .getOrDefault(changeMCP.getUrn().toString(), Collections.emptyMap())
            .getOrDefault(changeMCP.getAspectName(), null);

    // A new changeMCP would be versioned with the version after the current row 0
    changeMCP.setNextAspectVersion(nextVersion + 1);

    // support inner-batch upserts
    latestAspects
        .computeIfAbsent(changeMCP.getUrn().toString(), key -> new HashMap<>())
        .put(changeMCP.getAspectName(), databaseUpsert.apply(changeMCP, currentSystemAspect));
    nextVersions
        .computeIfAbsent(changeMCP.getUrn().toString(), key -> new HashMap<>())
        .put(changeMCP.getAspectName(), changeMCP.getNextAspectVersion());

    return changeMCP;
  }
}
