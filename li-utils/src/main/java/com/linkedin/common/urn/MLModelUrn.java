package com.linkedin.common.urn;

import java.net.URISyntaxException;

import com.linkedin.common.FabricType;

import static com.linkedin.common.urn.UrnUtils.toFabricType;


public final class MLModelUrn extends Urn {

  public static final String ENTITY_TYPE = "mlModel";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final DataPlatformUrn platformEntity;

  private final String mlModelNameEntity;

  private final FabricType originEntity;

  public MLModelUrn(DataPlatformUrn platform, String mlModelName, FabricType origin) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), mlModelName, origin.name()));
    this.platformEntity = platform;
    this.mlModelNameEntity = mlModelName;
    this.originEntity = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return platformEntity;
  }

  public String getMlModelNameEntity() {
    return mlModelNameEntity;
  }

  public FabricType getOriginEntity() {
    return originEntity;
  }

  public static MLModelUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new MLModelUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
  }

  public static MLModelUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
