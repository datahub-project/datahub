package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BulkProcessorConfiguration {
  private int numRetries;

  /**
   * Socket timeout in seconds for OpenSearch {@code RequestOptions} on by-query class operations
   * (delete/update-by-query, etc.). Same value for GMS and MAE processes; MAE-only tuning applies
   * to RestClient ({@code maeConsumer.elasticsearch.*}), not this setting. Not used for
   * system-update build-indices / reindex flows—those use {@link BuildIndicesConfiguration}.
   */
  private int slowByQueryOperationTimeoutSeconds;
}
