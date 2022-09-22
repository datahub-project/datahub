package com.linkedin.datahub.graphql.featureflags;

import lombok.Data;


@Data
public class FeatureFlags {
  private boolean showSimplifiedHomepageByDefault = false;
  private boolean lineageSearchCacheEnabled = false;
}
