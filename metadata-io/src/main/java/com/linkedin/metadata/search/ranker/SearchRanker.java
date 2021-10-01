package com.linkedin.metadata.search.ranker;

import com.google.common.collect.Streams;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;


public abstract class SearchRanker {

  public abstract List<FeatureExtractor> getFeatureExtractors();

  public abstract Comparable<?> score(SearchEntity searchEntity);

  /**
   * Rank the input list of entities
   */
  public List<SearchEntity> rank(List<SearchEntity> originalList) {
    return Streams.zip(originalList.stream(), getFeatures(originalList).stream(), this::updateFeatures)
        .map(entity -> new ScoredEntity(entity, score(entity)))
        .sorted(Comparator.<ScoredEntity, Comparable>comparing(ScoredEntity::getScore).reversed())
        .map(ScoredEntity::getEntity)
        .collect(Collectors.toList());
  }

  private List<Features> getFeatures(List<SearchEntity> originalList) {
    List<Features> originalFeatures =
        originalList.stream().map(SearchEntity::getFeatures).map(Features::from).collect(Collectors.toList());
    return ConcurrencyUtils.transformAndCollectAsync(getFeatureExtractors(),
        extractor -> extractor.extractFeatures(originalList)).stream().reduce(originalFeatures, Features::merge);
  }

  private SearchEntity updateFeatures(SearchEntity originalEntity, Features features) {
    return new SearchEntity().setEntity(originalEntity.getEntity())
        .setMatchedFields(originalEntity.getMatchedFields())
        .setFeatures(new DoubleMap(features.getNumericFeatures()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue))));
  }

  @Value
  protected static class ScoredEntity {
    SearchEntity entity;
    // Takes in any comparable object. Ranker uses it to order it in a descending manner
    Comparable<?> score;
  }
}
