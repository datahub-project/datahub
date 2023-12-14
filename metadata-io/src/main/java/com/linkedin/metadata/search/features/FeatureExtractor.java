package com.linkedin.metadata.search.features;

import com.linkedin.metadata.search.SearchEntity;
import java.util.List;

/** Interface for extractors that extract Features for each entity returned by search */
public interface FeatureExtractor {
  /** Return the extracted features for each entity returned by search */
  List<Features> extractFeatures(List<SearchEntity> entities);
}
