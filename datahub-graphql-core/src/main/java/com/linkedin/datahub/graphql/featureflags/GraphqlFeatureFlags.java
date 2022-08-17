package com.linkedin.datahub.graphql.featureflags;

public class GraphqlFeatureFlags {
  public boolean defaultShowSimplifiedHomepage = false;

  private static GraphqlFeatureFlags _featureFlags = new GraphqlFeatureFlags();

  public void setDefaultShowSimplifiedHomepage(boolean value) {
    defaultShowSimplifiedHomepage = value;
  }

  public static void setFeatureFlags(GraphqlFeatureFlags featureFlags) {
    _featureFlags = featureFlags;
  }

  public static boolean getDefaultShowSimplifiedHomepage() {
    return _featureFlags.defaultShowSimplifiedHomepage;
  }
}
