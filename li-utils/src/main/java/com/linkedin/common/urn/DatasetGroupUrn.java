package com.linkedin.common.urn;

import java.net.URISyntaxException;

import static com.linkedin.common.urn.UrnUtils.toFabricType;

public final class DatasetGroupUrn extends Urn {

  public static final String ENTITY_TYPE = "datasetGroup";

  private static final String CONTENT_FORMAT = "(%s,%s)";

  private final String namespaceEntity;

  private final String nameEntity;

  public DatasetGroupUrn(String namespace, String name) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, namespace, name));
    this.namespaceEntity = namespace;
    this.nameEntity = name;
  }

  public String getNamespaceEntity() {
    return namespaceEntity;
  }

  public String getNameEntity() {
    return nameEntity;
  }

  public static DatasetGroupUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length()-1).split(",");
    return new DatasetGroupUrn(parts[0], parts[1]);
  }

  public static DatasetGroupUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
