package datahub.client.patch;

import lombok.Getter;


public enum PatchOperationType {
  ADD("add"),
  REMOVE("remove"),
  REPLACE("replace"),
  COPY("copy"),
  MOVE("move"),
  TEST("test");

  @Getter
  private final String value;

  PatchOperationType(String value) {
    this.value = value;
  }

}
