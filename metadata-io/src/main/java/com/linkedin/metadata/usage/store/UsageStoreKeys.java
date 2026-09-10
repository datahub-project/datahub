package com.linkedin.metadata.usage.store;

import io.datahubproject.metadata.context.usage.UsageActorClass;
import java.util.Map;
import java.util.Objects;

record AdditiveRollupKey(
    String windowId,
    String metricName,
    UsageActorClass actorClass,
    Map<String, String> dimensions) {

  AdditiveRollupKey {
    Objects.requireNonNull(windowId);
    Objects.requireNonNull(metricName);
    Objects.requireNonNull(actorClass);
    Objects.requireNonNull(dimensions);
  }
}

/** One in-memory bucket per distinct metric and {@code actor_class} within a flush window. */
record DistinctRollupKey(String windowId, String metricName, String actorClass) {

  DistinctRollupKey {
    Objects.requireNonNull(windowId);
    Objects.requireNonNull(metricName);
    Objects.requireNonNull(actorClass);
  }
}
