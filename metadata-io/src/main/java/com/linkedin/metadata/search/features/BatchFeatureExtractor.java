package com.linkedin.metadata.search.features;

import com.google.common.collect.Lists;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Base class to extract features in batches
 */
public abstract class BatchFeatureExtractor implements FeatureExtractor {

  public abstract int getBatchSize();

  public abstract List<Features> extractFeaturesForBatch(List<SearchEntity> batch);

  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    return ConcurrencyUtils.transformAndCollectAsync(Lists.partition(entities, getBatchSize()),
        this::extractFeaturesForBatch).stream().flatMap(List::stream).collect(Collectors.toList());
  }
}
