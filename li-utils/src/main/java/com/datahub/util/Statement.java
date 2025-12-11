/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.util;

import java.util.Collections;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

@Value
public class Statement {

  String commandText;

  Map<String, Object> params;

  public Statement(@NonNull String commandText, @NonNull Map<String, Object> params) {
    this.commandText = commandText;
    this.params = Collections.unmodifiableMap(params);
  }
}
