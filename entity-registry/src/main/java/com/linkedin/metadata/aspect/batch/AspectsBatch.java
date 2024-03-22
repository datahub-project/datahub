package com.linkedin.metadata.aspect.batch;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * A batch of aspects in the context of either an MCP or MCL write path to a data store. The item is
 * a record that encapsulates the change type, raw aspect and ancillary information like {@link
 * SystemMetadata} and record/message created time
 */
public interface AspectsBatch {
  Collection<? extends BatchItem> getItems();

  AspectRetriever getAspectRetriever();

  /**
   * Returns MCP items. Could be patch, upsert, etc.
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
   * @param latestAspects latest version in the database
   * @return The new urn/aspectnames and the uniform upserts, possibly expanded/mutated by the
   *     various hooks
   */
  Pair<Map<String, Set<String>>, List<ChangeMCP>> toUpsertBatchItems(
      Map<String, Map<String, SystemAspect>> latestAspects);

  /**
   * Apply read mutations to batch
   *
   * @param items
   */
  default void applyReadMutationHooks(Collection<ReadItem> items) {
    applyReadMutationHooks(items, getAspectRetriever());
  }

  static void applyReadMutationHooks(Collection<ReadItem> items, AspectRetriever aspectRetriever) {
    for (MutationHook mutationHook : aspectRetriever.getEntityRegistry().getAllMutationHooks()) {
      mutationHook.applyReadMutation(items, aspectRetriever);
    }
  }

  /**
   * Apply write mutations to batch
   *
   * @param changeMCPS
   */
  default void applyWriteMutationHooks(Collection<ChangeMCP> changeMCPS) {
    applyWriteMutationHooks(changeMCPS, getAspectRetriever());
  }

  static void applyWriteMutationHooks(
      Collection<ChangeMCP> changeMCPS, AspectRetriever aspectRetriever) {
    for (MutationHook mutationHook : aspectRetriever.getEntityRegistry().getAllMutationHooks()) {
      mutationHook.applyWriteMutation(changeMCPS, aspectRetriever);
    }
  }

  default <T extends BatchItem> ValidationExceptionCollection validateProposed(
      Collection<T> mcpItems) {
    return validateProposed(mcpItems, getAspectRetriever());
  }

  static <T extends BatchItem> ValidationExceptionCollection validateProposed(
      Collection<T> mcpItems, AspectRetriever aspectRetriever) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    aspectRetriever.getEntityRegistry().getAllAspectPayloadValidators().stream()
        .flatMap(validator -> validator.validateProposed(mcpItems, aspectRetriever))
        .forEach(exceptions::addException);
    return exceptions;
  }

  default ValidationExceptionCollection validatePreCommit(Collection<ChangeMCP> changeMCPs) {
    return validatePreCommit(changeMCPs, getAspectRetriever());
  }

  static ValidationExceptionCollection validatePreCommit(
      Collection<ChangeMCP> changeMCPs, AspectRetriever aspectRetriever) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    aspectRetriever.getEntityRegistry().getAllAspectPayloadValidators().stream()
        .flatMap(validator -> validator.validatePreCommit(changeMCPs, aspectRetriever))
        .forEach(exceptions::addException);
    return exceptions;
  }

  default Stream<ChangeMCP> applyMCPSideEffects(Collection<ChangeMCP> items) {
    return applyMCPSideEffects(items, getAspectRetriever());
  }

  static Stream<ChangeMCP> applyMCPSideEffects(
      Collection<ChangeMCP> items, AspectRetriever aspectRetriever) {
    return aspectRetriever.getEntityRegistry().getAllMCPSideEffects().stream()
        .flatMap(mcpSideEffect -> mcpSideEffect.apply(items, aspectRetriever));
  }

  default Stream<MCLItem> applyMCLSideEffects(Collection<MCLItem> items) {
    return applyMCLSideEffects(items, getAspectRetriever());
  }

  static Stream<MCLItem> applyMCLSideEffects(
      Collection<MCLItem> items, AspectRetriever aspectRetriever) {
    return aspectRetriever.getEntityRegistry().getAllMCLSideEffects().stream()
        .flatMap(mclSideEffect -> mclSideEffect.apply(items, aspectRetriever));
  }

  default boolean containsDuplicateAspects() {
    return getItems().stream()
            .map(i -> String.format("%s_%s", i.getClass().getName(), i.hashCode()))
            .distinct()
            .count()
        != getItems().size();
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
    return Stream.concat(a.entrySet().stream(), b.entrySet().stream())
        .flatMap(
            entry ->
                entry.getValue().entrySet().stream()
                    .map(innerEntry -> Pair.of(entry.getKey(), innerEntry)))
        .collect(
            Collectors.groupingBy(
                Pair::getKey,
                Collectors.mapping(
                    Pair::getValue, Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }
}
