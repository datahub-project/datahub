package com.linkedin.metadata.search.ranker;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.features.NumEntitiesPerTypeFeature;
import com.linkedin.metadata.search.features.UsageFeature;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;


public class UsageBasedRanker extends SearchRanker {

  private final List<FeatureExtractor> featureExtractors;

  public UsageBasedRanker(NumEntitiesPerTypeFeature numEntitiesPerTypeFeature, UsageFeature usageFeature) {
    featureExtractors = ImmutableList.of(numEntitiesPerTypeFeature, usageFeature);
  }

  @Override
  public List<FeatureExtractor> getFeatureExtractors() {
    return featureExtractors;
  }

  @Override
  public Comparable<?> score(SearchEntity searchEntity) {
    Features features = Features.from(searchEntity.getFeatures());
    return Triple.of(features.getNumericFeature(Features.Name.QUERY_COUNT, 0.0),
        -features.getNumericFeature(Features.Name.RANK_WITHIN_TYPE, 0.0),
        features.getNumericFeature(Features.Name.NUM_ENTITIES_PER_TYPE, 0.0));
  }
}
