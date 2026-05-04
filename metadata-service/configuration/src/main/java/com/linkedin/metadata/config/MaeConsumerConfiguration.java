package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Elasticsearch tuning for {@code metadata-jobs/mae-consumer-job} and embedded MAE in GMS. Defaults
 * and env-backed placeholders live only in {@code application.yaml} ({@code maeConsumer.*}). Covers
 * MAE-specific <strong>RestClient</strong> timeouts only ({@code socketTimeoutMs}, {@code
 * connectionRequestTimeoutMs}); OpenSearch by-query {@code RequestOptions} use {@code
 * elasticsearch.bulkProcessor.slowByQueryOperationTimeoutSeconds} (same for GMS and MAE). Integer
 * fields {@code >= 0} override {@code elasticsearch.*}; {@code -1} defers to global Elasticsearch
 * client settings.
 */
@Data
public class MaeConsumerConfiguration {

  /**
   * Mirrors {@code MAE_CONSUMER_ENABLED}; when true, {@code maeConsumer.elasticsearch} RestClient
   * timeouts merge with {@code elasticsearch.*} on the shared {@code SearchClientShim}.
   */
  private Boolean enabled;

  private Elasticsearch elasticsearch;

  @Data
  public static class Elasticsearch {
    /** RestClient socket timeout (ms); {@code -1} uses {@code elasticsearch.socketTimeout}. */
    private Integer socketTimeoutMs;

    /**
     * Connection-pool checkout timeout (ms); {@code -1} uses {@code
     * elasticsearch.connectionRequestTimeout}.
     */
    private Integer connectionRequestTimeoutMs;
  }
}
