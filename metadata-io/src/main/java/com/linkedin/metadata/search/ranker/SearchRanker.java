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


/**
 * In memory ranker that re-ranks results returned by the search backend
 */
public abstract class SearchRanker {

  /**
   * List of feature extractors to use to fetch features for each entity returned by search backend
   */
  public abstract List<FeatureExtractor> getFeatureExtractors();

  /**
   * Return a comparable score for each entity returned by search backend. The ranker will rank based on this score
   */
  public abstract Comparable<?> score(SearchEntity searchEntity);

  /**
   * Rank the input list of entities
   */
  public List<SearchEntity> rank(List<SearchEntity> originalList) {
    return Streams.zip(originalList.stream(), fetchFeatures(originalList).stream(), this::updateFeatures)
        .map(entity -> new ScoredEntity(entity, score(entity)))
        .sorted(Comparator.<ScoredEntity, Comparable>comparing(ScoredEntity::getScore).reversed())
        .map(ScoredEntity::getEntity)
        .collect(Collectors.toList());
  }

  /**
   * Fetch features for each entity returned using the feature extractors
   */
  private List<Features> fetchFeatures(List<SearchEntity> originalList) {
    List<Features> originalFeatures =
        originalList.stream().map(SearchEntity::getFeatures).map(Features::from).collect(Collectors.toList());
    return ConcurrencyUtils.transformAndCollectAsync(getFeatureExtractors(),
        extractor -> extractor.extractFeatures(originalList)).stream().reduce(originalFeatures, Features::merge);
  }

  /**
   * Add the extracted features into each search entity to return the features in the response
   */
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
