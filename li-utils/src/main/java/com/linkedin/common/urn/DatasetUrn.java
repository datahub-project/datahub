package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import java.net.URISyntaxException;

import static com.linkedin.common.urn.UrnUtils.*;


public final class DatasetUrn extends Urn {

  public static final String ENTITY_TYPE = "dataset";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final DataPlatformUrn platformEntity;

  private final String datasetNameEntity;

  private final FabricType originEntity;

  public DatasetUrn(DataPlatformUrn platform, String name, FabricType origin) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), name, origin.name()));
    this.platformEntity = platform;
    this.datasetNameEntity = name;
    this.originEntity = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return platformEntity;
  }

  public String getDatasetNameEntity() {
    return datasetNameEntity;
  }

  public FabricType getOriginEntity() {
    return originEntity;
  }

  public static DatasetUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length()-1).split(",");
    return new DatasetUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
  }

  public static DatasetUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
