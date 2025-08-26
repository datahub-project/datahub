package com.linkedin.metadata.datahubusage.event;

import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.SSO_SCIM_SOURCE;

import javax.annotation.Nullable;
import lombok.Getter;

public enum EventSource {
  RESTLI("RESTLI"),
  OPENAPI("OPENAPI"),
  GRAPHQL("GRAPHQL"),
  SSO_SCIM(SSO_SCIM_SOURCE);

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
