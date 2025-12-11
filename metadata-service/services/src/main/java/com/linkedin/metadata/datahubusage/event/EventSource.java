/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.datahubusage.event;

import javax.annotation.Nullable;
import lombok.Getter;

public enum EventSource {
  RESTLI("RESTLI"),
  OPENAPI("OPENAPI"),
  GRAPHQL("GRAPHQL");

  @Getter private final String source;

  EventSource(String source) {
    this.source = source;
  }

  @Nullable
  public static EventSource getSource(String name) {
    for (EventSource eventSource : EventSource.values()) {
      if (eventSource.source.equalsIgnoreCase(name)) {
        return eventSource;
      }
    }
    return null;
  }
}
