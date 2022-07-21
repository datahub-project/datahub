package com.linkedin.metadata.search.ranker;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.features.FeatureExtractor;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.features.MatchMetadataFeature;
import java.util.List;
import org.javatuples.Pair;


public class DefaultRanker extends SearchRanker {

  private final List<FeatureExtractor> featureExtractors;

  public DefaultRanker() {
    featureExtractors = ImmutableList.of(new MatchMetadataFeature());
  }

  @Override
  public List<FeatureExtractor> getFeatureExtractors() {
    return featureExtractors;
  }

  @Override
  public Comparable<?> score(SearchEntity searchEntity) {
    Features features = Features.from(searchEntity.getFeatures());
    return Pair.with(-features.getNumericFeature(
        Features.Name.ONLY_MATCH_CUSTOM_PROPERTIES, 0.0),
        features.getNumericFeature(Features.Name.SEARCH_BACKEND_SCORE, 0.0));
  }
}
