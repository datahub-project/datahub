package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.SearchEntity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class NumEntitiesPerTypeFeature implements FeatureExtractor {
  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    Map<String, List<SearchEntity>> entitiesPerType =
        entities.stream().collect(Collectors.groupingBy(entity -> entity.getEntity().getEntityType()));
    Map<Urn, Integer> rankForTypePerEntity = new HashMap<>();

    for (Map.Entry<String, List<SearchEntity>> entry : entitiesPerType.entrySet()) {
      for (int i = 0; i < entry.getValue().size(); i++) {
        rankForTypePerEntity.put(entry.getValue().get(i).getEntity(), i);
      }
    }

    return entities.stream()
        .map(entity -> new Features(ImmutableMap.of(Features.Name.NUM_ENTITIES_PER_TYPE,
            (double) entitiesPerType.get(entity.getEntity().getEntityType()).size(), Features.Name.RANK_WITHIN_TYPE,
            rankForTypePerEntity.get(entity.getEntity()).doubleValue())))
        .collect(Collectors.toList());
  }
}
