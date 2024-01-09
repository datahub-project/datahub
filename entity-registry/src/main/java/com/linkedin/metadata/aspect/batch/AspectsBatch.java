package com.linkedin.metadata.aspect.batch;

import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
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
  List<? extends BatchItem> getItems();

  /**
   * Returns MCP items. Can be patch, upsert, etc.
   *
   * @return batch items
   */
  default List<? extends MCPBatchItem> getMCPItems() {
    return getItems().stream()
        .filter(item -> item instanceof MCPBatchItem)
        .map(item -> (MCPBatchItem) item)
        .collect(Collectors.toList());
  }

  Pair<Map<String, Set<String>>, List<UpsertItem>> toUpsertBatchItems(
      Map<String, Map<String, SystemAspect>> latestAspects,
      EntityRegistry entityRegistry,
      AspectRetriever aspectRetriever);

  default Stream<UpsertItem> applyMCPSideEffects(
      List<UpsertItem> items, EntityRegistry entityRegistry, AspectRetriever aspectRetriever) {
    return entityRegistry.getAllMCPSideEffects().stream()
        .flatMap(mcpSideEffect -> mcpSideEffect.apply(items, entityRegistry, aspectRetriever));
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
        .map(aspect -> Map.entry(aspect.getUrn().toString(), aspect.getAspectName()))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));
  }

  default Map<String, Set<String>> getNewUrnAspectsMap(
      Map<String, Set<String>> existingMap, List<? extends BatchItem> items) {
    Map<String, HashSet<String>> newItemsMap =
        items.stream()
            .map(aspect -> Map.entry(aspect.getUrn().toString(), aspect.getAspectName()))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(
                        Map.Entry::getValue, Collectors.toCollection(HashSet::new))));

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

  default <T> Map<String, Map<String, T>> merge(
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
