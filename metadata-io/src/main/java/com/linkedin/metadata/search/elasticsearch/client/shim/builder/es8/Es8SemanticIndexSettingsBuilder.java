package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

public final class Es8SemanticIndexSettingsBuilder {

  private Es8SemanticIndexSettingsBuilder() {}

  @Nonnull
  public static Map<String, Object> build(@Nonnull SemanticIndexSpec spec) {
    Map<String, Object> settings = new LinkedHashMap<>();
    settings.put("number_of_shards", 1);
    settings.put("number_of_replicas", 1);
    return settings;
  }
}
