package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.AspectSpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class MappingsBuilder {

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final AspectSpec aspectSpec) {
    if (!aspectSpec.isTimeseries()) {
      throw new IllegalArgumentException(
          String.format("Cannot apply temporal stats indexing for a non-temporal aspect %s", aspectSpec.getName()));
    }

    Map<String, Object> mappings = new HashMap<>();

    mappings.put("urn", ImmutableMap.of("type", "keyword"));
    mappings.put("timestampMillis", ImmutableMap.of("type", "date"));
    mappings.put("event", ImmutableMap.of("type", "object", "enabled", false));

    return mappings;
  }
}
