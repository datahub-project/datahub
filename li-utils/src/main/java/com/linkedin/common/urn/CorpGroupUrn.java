package com.linkedin.common.urn;

public final class CorpGroupUrn extends Urn {

  public static final String ENTITY_TYPE = "corpGroup";

  private final String groupNameEntity;

  public CorpGroupUrn(String groupName) {
    super(ENTITY_TYPE, groupName);
    this.groupNameEntity = groupName;
  }

  public String getGroupNameEntity() {
    return groupNameEntity;
  }
}
