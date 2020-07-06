package com.linkedin.common.urn;

import java.net.URISyntaxException;

import com.linkedin.common.FabricType;

import static com.linkedin.common.urn.UrnUtils.toFabricType;


public final class ModelUrn extends Urn {

  public static final String ENTITY_TYPE = "model";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final DataPlatformUrn platformEntity;

  private final String modelNameEntity;

  private final FabricType originEntity;

  public ModelUrn(DataPlatformUrn platform, String name, FabricType origin) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), name, origin.name()));
    this.platformEntity = platform;
    this.modelNameEntity = name;
    this.originEntity = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return platformEntity;
  }

  public String getModelNameEntity() {
    return modelNameEntity;
  }

  public FabricType getOriginEntity() {
    return originEntity;
  }

  public static ModelUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new ModelUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
  }

  public static ModelUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
