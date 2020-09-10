package com.linkedin.common.urn;

public final class MLFeatureUrn extends Urn {

  public static final String ENTITY_TYPE = "mlFeature";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final String mlFeatureNamespace;

  private final String mlFeatureName;

  public MLFeatureUrn(String mlFeatureNamespace, String mlFeatureName) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, mlFeatureNamespace, mlFeatureName));
    this.mlFeatureNamespace = mlFeatureNamespace;
    this.mlFeatureName = mlFeatureName;
  }

  public String getMlFeatureName() {
    return mlFeatureName;
  }

  public String getMlFeatureNamespace() {
    return mlFeatureNamespace;
  }

}
