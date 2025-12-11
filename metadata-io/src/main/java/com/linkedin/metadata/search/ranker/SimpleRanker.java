/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
