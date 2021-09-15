package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.SearchEntity;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class NumEntitiesPerTypeFeature implements FeatureExtractor {
  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    Map<String, Long> numEntitiesPerType = entities.stream()
        .collect(Collectors.groupingBy(entity -> entity.getEntity().getEntityType(), Collectors.counting()));
    return entities.stream()
        .map(entity -> new Features(ImmutableMap.of(Features.Name.NUM_ENTITIES_PER_TYPE,
            numEntitiesPerType.getOrDefault(entity.getEntity().getEntityType(), 0L).doubleValue())))
        .collect(Collectors.toList());
  }
}
