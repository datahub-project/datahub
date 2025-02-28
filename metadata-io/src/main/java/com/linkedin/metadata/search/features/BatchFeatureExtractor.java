package com.linkedin.metadata.search.features;

import com.google.common.collect.Lists;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Base class to extract features in batches */
public abstract class BatchFeatureExtractor implements FeatureExtractor {

  public abstract int getBatchSize();

  public abstract List<Features> extractFeaturesForBatch(
      @Nonnull OperationContext opContext, List<SearchEntity> batch);

  @Override
  public List<Features> extractFeatures(
      @Nonnull OperationContext opContext, List<SearchEntity> entities) {
    return ConcurrencyUtils.transformAndCollectAsync(
            Lists.partition(entities, getBatchSize()),
            batch -> extractFeaturesForBatch(opContext, batch))
        .stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
