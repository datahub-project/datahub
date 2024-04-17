package com.linkedin.metadata.search.features;

import com.linkedin.metadata.search.SearchEntity;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/** Interface for extractors that extract Features for each entity returned by search */
public interface FeatureExtractor {
  /** Return the extracted features for each entity returned by search */
  List<Features> extractFeatures(@Nonnull OperationContext opContext, List<SearchEntity> entities);
}
