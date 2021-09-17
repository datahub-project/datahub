package com.linkedin.metadata.search.features;

import com.linkedin.metadata.search.SearchEntity;
import java.util.List;


public interface FeatureExtractor {
  List<Features> extractFeatures(List<SearchEntity> entities);
}
