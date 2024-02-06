package com.linkedin.metadata.aspect.patch;

import lombok.Getter;

public enum PatchOperationType {
  ADD("add"),
  REMOVE("remove");
  // The other operations do not work as expected, the limited list was to avoid usage of unintended
  // operations.

  @Getter private final String value;

  PatchOperationType(String value) {
    this.value = value;
  }
}
