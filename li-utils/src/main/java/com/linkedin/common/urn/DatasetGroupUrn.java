package com.linkedin.common.urn;

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
}
