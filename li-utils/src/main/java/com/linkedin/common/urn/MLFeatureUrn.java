package com.linkedin.common.urn;

public final class MLFeatureUrn extends Urn {

  public static final String ENTITY_TYPE = "mlFeature";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final String featureNamespace;

  private final String featureName;

  public MLFeatureUrn(String featureNameSpace, String featureName) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, featureNameSpace, featureName));
    this.featureNamespace = featureNameSpace;
    this.featureName = featureName;
  }

  public String getFeatureName() {
    return featureName;
  }

  public String getFeatureNamespace() {
    return featureNamespace;
  }

}
