package com.linkedin.metadata.config.graph;

import com.linkedin.metadata.config.shared.LimitConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class GraphServiceConfiguration {
  private String type;
  private LimitConfig limit;

  /**
   * Upper bound on edges per chunk for batched graph mutations ({@code addEdges}, {@code
   * upsertEdges}, {@code removeEdges}) when the implementation applies chunking. Default is defined
   * in {@code application.yaml} ({@code graphService.maxEdgeBatchSize}). PostgreSQL graph JDBC
   * batching uses the minimum of this value and {@code postgres.pgGraph.maxEdgeWriteBatchSize}. Use
   * {@code 0} to apply only the PostgreSQL limit.
   */
  private int maxEdgeBatchSize;
}
