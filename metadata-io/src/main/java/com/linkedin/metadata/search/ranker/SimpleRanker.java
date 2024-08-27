package com.linkedin.metadata.search.ranker;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import java.util.List;
import java.util.Optional;

/**
 * Simple ranker that diversifies the results between different entities. For the same entity,
 * returns the same order from elasticsearch
 */
public class SimpleRanker extends SearchRanker<Double> {

  private final List<FeatureExtractor> featureExtractors;

  public SimpleRanker() {
    featureExtractors = ImmutableList.of();
  }

  @Override
  public List<FeatureExtractor> getFeatureExtractors() {
    return featureExtractors;
  }

  @Override
  public Double score(SearchEntity searchEntity) {
    return Optional.ofNullable(searchEntity.getScore()).orElse(0.0);
  }
}
