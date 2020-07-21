package com.linkedin.common.urn;

public final class MlFeatureUrn extends Urn {

  public static final String ENTITY_TYPE = "mlFeature";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final String featureName;

  private final String dataType;

  public MlFeatureUrn(String featureName, String dataType) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, featureName, dataType));
    this.featureName = featureName;
    this.dataType = dataType;
  }

  public String getFeatureName() {
    return featureName;
  }

  public String getDataType() {
    return dataType;
  }

}
