/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
