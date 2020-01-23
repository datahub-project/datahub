package com.linkedin.common.urn;

public final class FabricUrn extends Urn {

  public static final String ENTITY_TYPE = "fabric";

  private final String nameEntity;

  public FabricUrn(String name) {
    super(ENTITY_TYPE, name);
    this.nameEntity = name;
  }

  public String getNameEntity() {
    return nameEntity;
  }
}
