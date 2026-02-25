package datahub.client.v2.config;

/**
 * Controls how metadata changes are persisted and indexed in DataHub.
 *
 * <p>EmitMode determines the consistency vs performance tradeoff for metadata operations:
 *
 * <ul>
 *   <li><b>SYNC_WAIT</b>: Fully synchronous - updates both primary storage (SQL) and search storage
 *       (Elasticsearch) before returning. Strongest consistency but highest cost. Use for critical
 *       operations requiring immediate searchability.
 *   <li><b>SYNC_PRIMARY</b> (default): Balanced - synchronously updates primary storage (SQL) but
 *       asynchronously updates search storage (Elasticsearch). Changes are immediately visible via
 *       direct entity reads but may have slight delay in search results.
 *   <li><b>ASYNC</b>: Queues the change for asynchronous processing and returns immediately. Best
 *       for high-throughput scenarios where eventual consistency is acceptable.
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * // Use SYNC_WAIT for operations that need immediate search visibility
 * DataHubClientConfigV2 config = DataHubClientConfigV2.builder()
 *     .server("http://localhost:8080")
 *     .emitMode(EmitMode.SYNC_WAIT)
 *     .build();
 *
 * // Use SYNC_PRIMARY (default) for balanced performance
 * DataHubClientConfigV2 config = DataHubClientConfigV2.builder()
 *     .server("http://localhost:8080")
 *     // .emitMode defaults to SYNC_PRIMARY
 *     .build();
 *
 * // Use ASYNC for bulk ingestion pipelines
 * DataHubClientConfigV2 config = DataHubClientConfigV2.builder()
 *     .server("http://localhost:8080")
 *     .emitMode(EmitMode.ASYNC)
 *     .build();
 * </pre>
 */
public enum EmitMode {
  /**
   * Fully synchronous processing that updates both primary storage (SQL) and search storage
   * (Elasticsearch) before returning.
   *
   * <p>Provides the strongest consistency guarantee but with the highest cost. Best for critical
   * operations where immediate searchability and consistent reads are required.
   *
   * <p>Implementation: Sets async=false and X-DataHub-Sync-Index-Update: true
   */
  SYNC_WAIT,

  /**
   * Synchronously updates the primary storage (SQL) but asynchronously updates search storage
   * (Elasticsearch).
   *
   * <p>Provides a balance between consistency and performance. Suitable for updates that need to be
   * immediately reflected in direct entity retrievals but where search index consistency can be
   * slightly delayed.
   *
   * <p>This is the default mode for SDK operations.
   *
   * <p>Implementation: Sets async=false
   */
  SYNC_PRIMARY,

  /**
   * Queues the metadata change for asynchronous processing and returns immediately.
   *
   * <p>The client continues execution without waiting for the change to be fully processed. Best
   * for high-throughput scenarios where eventual consistency is acceptable.
   *
   * <p>Implementation: Sets async=true
   */
  ASYNC
}
