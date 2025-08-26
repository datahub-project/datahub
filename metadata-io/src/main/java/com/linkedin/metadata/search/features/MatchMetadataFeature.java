package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.SearchEntity;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class MatchMetadataFeature implements FeatureExtractor {
  private static final String CUSTOM_PROPERTIES = "customProperties";

  @Override
  public List<Features> extractFeatures(
      @Nonnull OperationContext opContext, List<SearchEntity> entities) {
    return entities.stream().map(this::getFeature).collect(Collectors.toList());
  }

  private Features getFeature(SearchEntity entity) {
    return new Features(
        ImmutableMap.of(
            Features.Name.ONLY_MATCH_CUSTOM_PROPERTIES,
            onlyMatchCustomProperties(entity) ? 1.0 : 0.0));
  }

  private boolean onlyMatchCustomProperties(SearchEntity entity) {
    return !entity.getMatchedFields().isEmpty()
        && entity.getMatchedFields().stream()
            .allMatch(field -> field.getName().equals(CUSTOM_PROPERTIES));
  }
}
