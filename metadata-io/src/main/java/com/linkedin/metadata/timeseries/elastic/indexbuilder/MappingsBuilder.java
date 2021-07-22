package com.linkedin.metadata.timeseries.elastic.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.AspectSpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class MappingsBuilder {

  public static final String URN_FIELD = "urn";
  public static final String TIMESTAMP_FIELD = "@timestamp";
  public static final String TIMESTAMP_MILLIS_FIELD = "timestampMillis";
  public static final String EVENT_FIELD = "event";
  public static final String SYSTEM_METADATA_FIELD = "systemMetadata";

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final AspectSpec aspectSpec) {
    if (!aspectSpec.isTimeseries()) {
      throw new IllegalArgumentException(
          String.format("Cannot apply temporal stats indexing for a non-temporal aspect %s", aspectSpec.getName()));
    }

    Map<String, Object> mappings = new HashMap<>();

    mappings.put(URN_FIELD, ImmutableMap.of("type", "keyword"));
    mappings.put(TIMESTAMP_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(TIMESTAMP_MILLIS_FIELD, ImmutableMap.of("type", "date"));
    mappings.put(EVENT_FIELD, ImmutableMap.of("type", "object", "enabled", false));
    mappings.put(SYSTEM_METADATA_FIELD, ImmutableMap.of("type", "object", "enabled", false));

    return ImmutableMap.of("properties", mappings);
  }
}
