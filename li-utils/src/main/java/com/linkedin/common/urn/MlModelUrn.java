package com.linkedin.common.urn;

import java.net.URISyntaxException;

import com.linkedin.common.FabricType;

import static com.linkedin.common.urn.UrnUtils.toFabricType;


public final class MlModelUrn extends Urn {

  public static final String ENTITY_TYPE = "model";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final DataPlatformUrn platformEntity;

  private final String modelNameEntity;

  private final FabricType originEntity;

  public MlModelUrn(DataPlatformUrn platform, String modelName, FabricType origin) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), modelName, origin.name()));
    this.platformEntity = platform;
    this.modelNameEntity = modelName;
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

  public static MlModelUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new MlModelUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
  }

  public static MlModelUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
