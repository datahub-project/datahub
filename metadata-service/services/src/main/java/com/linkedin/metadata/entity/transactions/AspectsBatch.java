package com.linkedin.metadata.entity.transactions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface AspectsBatch {
  List<? extends AbstractBatchItem> getItems();

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
}
