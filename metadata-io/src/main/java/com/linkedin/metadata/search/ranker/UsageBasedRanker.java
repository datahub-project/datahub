package com.linkedin.metadata.search.ranker;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.features.GraphBasedFeature;
import com.linkedin.metadata.search.features.UsageFeature;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;


public class UsageBasedRanker extends SearchRanker {

  private final List<FeatureExtractor> featureExtractors;

  public UsageBasedRanker(UsageFeature usageFeature, GraphBasedFeature graphBasedFeature) {
    featureExtractors = ImmutableList.of(usageFeature, graphBasedFeature);
  }

  @Override
  public List<FeatureExtractor> getFeatureExtractors() {
    return featureExtractors;
  }

  @Override
  public Comparable<?> score(SearchEntity searchEntity) {
    Features features = Features.from(searchEntity.getFeatures());
    return Triple.of(features.getNumericFeature(Features.Name.QUERY_COUNT, 0.0),
        features.getNumericFeature(Features.Name.OUT_DEGREE, 0.0),
        features.getNumericFeature(Features.Name.HAS_OWNERS, 0.0));
  }
}
