package com.linkedin.metadata.search.ranker;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.features.MatchMetadataFeature;
import com.linkedin.metadata.search.features.UsageFeature;
import java.util.List;
import org.javatuples.Triplet;


public class UsageBasedRanker extends SearchRanker {

  private final List<FeatureExtractor> featureExtractors;

  public UsageBasedRanker(UsageFeature usageFeature) {
    featureExtractors = ImmutableList.of(usageFeature, new MatchMetadataFeature());
  }

  @Override
  public List<FeatureExtractor> getFeatureExtractors() {
    return featureExtractors;
  }

  @Override
  public Comparable<?> score(SearchEntity searchEntity) {
    Features features = Features.from(searchEntity.getFeatures());
    return Triplet.with(-features.getNumericFeature(Features.Name.ONLY_MATCH_CUSTOM_PROPERTIES, 0.0),
        features.getNumericFeature(Features.Name.QUERY_COUNT, 0.0),
        features.getNumericFeature(Features.Name.HAS_OWNERS, 0.0));
  }
}
