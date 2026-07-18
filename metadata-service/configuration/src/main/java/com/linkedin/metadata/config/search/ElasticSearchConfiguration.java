package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.Nullable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ElasticSearchConfiguration {
  private BulkDeleteConfiguration bulkDelete;
  private BulkProcessorConfiguration bulkProcessor;
  private BuildIndicesConfiguration buildIndices;

  /**
   * When false, DataHub omits outbound OpenSearch/Elasticsearch wiring (PostgreSQL-backed search
   * and related beans). Default is defined in {@code application.yaml} ({@code
   * elasticsearch.enabled}).
   */
  private boolean enabled;

  private SearchConfiguration search;
  private String idHashAlgo;
  private IndexConfiguration index;
  private ScrollConfiguration scroll;
  private EntityIndexConfiguration entityIndex;
  private String host;
  private int port;
  private int threadCount;
  private int connectionRequestTimeout;
  private int socketTimeout;
  private String username;
  private String password;
  private String pathPrefix;
  private boolean useSSL;
  private boolean opensearchUseAwsIamAuth;
  private String region;

  /**
   * Effective v3 flag: raw {@link EntityIndexConfiguration#getV3()} may remain enabled while v3
   * indexing is inactive when {@link #enabled} is false.
   */
  public boolean isEffectiveEntityIndexV3Enabled() {
    return enabled
        && entityIndex != null
        && entityIndex.getV3() != null
        && entityIndex.getV3().isEnabled();
  }

  /**
   * View of {@link #entityIndex} for indexing behavior: treats v3 as off when elasticsearch
   * integration is off, without modifying the bound beans.
   */
  @Nullable
  public EntityIndexConfiguration effectiveEntityIndex() {
    if (entityIndex == null) {
      return null;
    }
    EntityIndexVersionConfiguration v3 = entityIndex.getV3();
    if (enabled || v3 == null || !v3.isEnabled()) {
      return entityIndex;
    }
    return entityIndex.toBuilder().v3(v3.toBuilder().enabled(false).build()).build();
  }
}
