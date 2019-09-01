package com.linkedin.common.urn;

import com.linkedin.common.RegisteredSchemaType;


public final class RegisteredSchemaUrn extends Urn {

  public static final String ENTITY_TYPE = "registeredSchema";

  private static final String CONTENT_FORMAT = "(%s,%s)";

  private final RegisteredSchemaType typeEntity;

  private final String nameEntity;

  public RegisteredSchemaUrn(RegisteredSchemaType type, String name) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, type, name));
    this.typeEntity = type;
    this.nameEntity = name;
  }

  public RegisteredSchemaType getTypeEntity() {
    return typeEntity;
  }

  public String getNameEntity() {
    return nameEntity;
  }
}
