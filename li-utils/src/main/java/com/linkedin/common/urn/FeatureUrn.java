package com.linkedin.common.urn;

public final class FeatureUrn extends Urn {

  public static final String ENTITY_TYPE = "model";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final String featureName;

  private final String description;

  private final String type;

  public FeatureUrn(String featureName, String name, String type) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, featureName, name, type));
    this.featureName = featureName;
    this.description = name;
    this.type = type;
  }

  public String getFeatureName() {
    return featureName;
  }

  public String getDescription() {
    return description;
  }

  public String getType() {
    return type;
  }

}
