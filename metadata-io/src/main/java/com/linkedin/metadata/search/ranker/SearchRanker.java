package com.linkedin.metadata.search.ranker;

import com.google.common.collect.Streams;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Value;


public abstract class SearchRanker {

  public abstract List<FeatureExtractor> getFeatureExtractors();

  public abstract Comparable<?> score(EntityWithFeatures entityWithFeatures);

  /**
   * Rank the input list of entities
   */
  public List<SearchEntity> rank(List<SearchEntity> originalList) {
    return Streams.zip(originalList.stream(), getFeatures(originalList).stream(), EntityWithFeatures::new)
        .map(entityWithFeatures -> new ScoredEntity(entityWithFeatures.getEntity(), score(entityWithFeatures)))
        .sorted(Comparator.<ScoredEntity, Comparable>comparing(ScoredEntity::getScore).reversed())
        .map(ScoredEntity::getEntity)
        .collect(Collectors.toList());
  }

  private List<Features> getFeatures(List<SearchEntity> originalList) {
    List<Features> originalFeatures =
        originalList.stream().map(SearchEntity::getFeatures).map(Features::from).collect(Collectors.toList());
    List<CompletableFuture<List<Features>>> extractedFeatures = getFeatureExtractors().stream()
        .map(extractor -> CompletableFuture.supplyAsync(() -> extractor.extractFeatures(originalList)))
        .collect(Collectors.toList());
    CompletableFuture.allOf(extractedFeatures.toArray(new CompletableFuture[0])).join();
    return extractedFeatures.stream().map(CompletableFuture::join).reduce(originalFeatures, Features::merge);
  }

  @Value
  private static class EntityWithFeatures {
    SearchEntity entity;
    Features features;
  }

  @Value
  private static class ScoredEntity {
    SearchEntity entity;
    // Takes in any comparable object. Ranker uses it to order it in a descending manner
    Comparable<?> score;
  }
}
