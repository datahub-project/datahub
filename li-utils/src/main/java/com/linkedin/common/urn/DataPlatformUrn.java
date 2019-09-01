package com.linkedin.common.urn;

import java.net.URISyntaxException;


public final class DataPlatformUrn extends Urn {

  public static final String ENTITY_TYPE = "dataPlatform";

  private final String platformNameEntity;

  public DataPlatformUrn(String platformName) {
    super(ENTITY_TYPE, platformName);
    this.platformNameEntity = platformName;
  }

  public String getPlatformNameEntity() {
    return platformNameEntity;
  }

  public static DataPlatformUrn createFromString(String rawUrn) throws URISyntaxException {
    String platformName = new Urn(rawUrn).getContent();
    return new DataPlatformUrn(platformName);
  }
}
