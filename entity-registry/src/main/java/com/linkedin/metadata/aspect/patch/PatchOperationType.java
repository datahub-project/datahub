package com.linkedin.metadata.aspect.patch;

import lombok.Getter;

public enum PatchOperationType {
  ADD("add"),
  REMOVE("remove"),
  REPLACE("replace"),
  MOVE("move"),
  COPY("copy"),
  TEST("test");

  @Getter private final String value;

  PatchOperationType(String value) {
    this.value = value;
  }
}
