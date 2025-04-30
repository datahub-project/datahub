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
