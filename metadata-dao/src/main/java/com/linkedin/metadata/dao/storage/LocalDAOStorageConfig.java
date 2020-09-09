package com.linkedin.metadata.dao.storage;

import com.linkedin.data.template.RecordTemplate;
import java.util.Map;
import lombok.Builder;
import lombok.Value;


/**
 * Immutable class that holds the storage config for different paths of different metadata aspects
 */
@Value
@Builder
public final class LocalDAOStorageConfig {

  /**
   * Map of corresponding {@link Class} of metadata aspect to {@link AspectStorageConfig} config
   */
  Map<Class<? extends RecordTemplate>, AspectStorageConfig> aspectStorageConfigMap;

  /**
   * Immutable class that holds the storage config of different pegasus paths of a given metadata aspect
   */
  @Value
  @Builder
  public final static class AspectStorageConfig {

    /**
     * Map of string representation of Pegasus Path to {@link PathStorageConfig} config
     */
    Map<String, PathStorageConfig> pathStorageConfigMap;
  }

  /**
   * Immutable class that holds the storage config of a given pegasus path of a given metadata aspect
   */
  @Value
  @Builder
  public final static class PathStorageConfig {

    /**
     * Whether to index the pegasus path to local secondary index
     */
    @Builder.Default
    boolean strongConsistentSecondaryIndex = false;
  }
}