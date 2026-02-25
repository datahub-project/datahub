package datahub.client.v2.config;

import datahub.client.rest.RestEmitterConfig;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Configuration for DataHub Client V2.
 *
 * <p>This configuration wraps the RestEmitterConfig and adds V2-specific settings.
 *
 * <p>Example usage:
 *
 * <pre>
 * DataHubClientConfigV2 config = DataHubClientConfigV2.builder()
 *     .server("http://localhost:8080")
 *     .token("my-token")
 *     .timeout(30000)
 *     .mode(OperationMode.SDK)
 *     .build();
 * </pre>
 */
@Value
@Builder
public class DataHubClientConfigV2 {

  /**
   * Operation mode for the SDK.
   *
   * <ul>
   *   <li>SDK mode (default): User-initiated edits that write to editable aspects (e.g.,
   *       editableDatasetProperties)
   *   <li>INGESTION mode: System/pipeline writes that write to system aspects (e.g.,
   *       datasetProperties)
   * </ul>
   */
  public enum OperationMode {
    /** SDK mode: User-initiated edits write to editable aspects. */
    SDK,
    /** INGESTION mode: System/pipeline writes write to system aspects. */
    INGESTION
  }

  /**
   * The operation mode for this client. Defaults to SDK mode.
   *
   * <p>SDK mode writes to editable aspects (e.g., editableDatasetProperties) while INGESTION mode
   * writes to system aspects (e.g., datasetProperties).
   */
  @Builder.Default @Nonnull OperationMode mode = OperationMode.SDK;

  /** The DataHub server URL (required). */
  @Nonnull String server;

  /** Authentication token (optional). */
  @Nullable String token;

  /** Request timeout in milliseconds. Default is 10000ms (10 seconds). */
  @Builder.Default int timeoutMs = 10000;

  /**
   * Whether to disable SSL certificate verification. Default is false. WARNING: Only use this for
   * testing!
   */
  @Builder.Default boolean disableSslVerification = false;

  /** Maximum number of retries for failed requests. Default is 3. */
  @Builder.Default int maxRetries = 3;

  /**
   * Controls how metadata changes are persisted and indexed. Default is SYNC_PRIMARY.
   *
   * <p>Emit mode determines the consistency vs performance tradeoff:
   *
   * <ul>
   *   <li>SYNC_WAIT: Updates both SQL and Elasticsearch synchronously (strongest consistency)
   *   <li>SYNC_PRIMARY (default): Updates SQL synchronously, Elasticsearch asynchronously
   *       (balanced)
   *   <li>ASYNC: Queues for async processing, returns immediately (highest throughput)
   * </ul>
   *
   * <p>See {@link EmitMode} for detailed descriptions.
   */
  @Builder.Default @Nonnull EmitMode emitMode = EmitMode.SYNC_PRIMARY;

  /**
   * Server config cache TTL in milliseconds. Default is 60000ms (60 seconds). Set to 0 to disable
   * caching, or -1 to cache indefinitely.
   */
  @Builder.Default long configRefreshIntervalMs = 60000;

  /**
   * Aspect cache TTL in milliseconds. Default is 60000ms (60 seconds).
   *
   * <p>This controls how long fetched aspects are cached before they expire and need to be
   * refetched from the server. Set to 0 to disable caching (always fetch from server), or -1 to
   * cache indefinitely.
   *
   * <p>A shorter TTL means more up-to-date data but more server requests. A longer TTL improves
   * performance but may serve stale data.
   */
  @Builder.Default long aspectCacheTtlMs = 60000;

  /**
   * Converts this V2 config to a RestEmitterConfig for use with the underlying emitter.
   *
   * @return the RestEmitterConfig
   */
  @Nonnull
  public RestEmitterConfig toRestEmitterConfig() {
    RestEmitterConfig.RestEmitterConfigBuilder builder =
        RestEmitterConfig.builder().server(server).timeoutSec(timeoutMs / 1000);

    if (token != null) {
      builder.token(token);
    }

    if (disableSslVerification) {
      builder.disableSslVerification(true);
    }

    // Map EmitMode to RestEmitter async flag
    builder.asyncIngest(emitMode == EmitMode.ASYNC);

    // Add headers
    java.util.Map<String, String> headers = new java.util.HashMap<>();
    // Add client mode header to communicate SDK/INGESTION mode to server
    headers.put("X-DataHub-Client-Mode", mode.name());
    // Add sync index update header for SYNC_WAIT mode
    if (emitMode == EmitMode.SYNC_WAIT) {
      headers.put("X-DataHub-Sync-Index-Update", "true");
    }
    builder.extraHeaders(headers);

    return builder.build();
  }

  /**
   * Creates a configuration from environment variables.
   *
   * <p>Supported environment variables: - DATAHUB_SERVER or DATAHUB_GMS_URL: Server URL -
   * DATAHUB_TOKEN or DATAHUB_GMS_TOKEN: Authentication token
   *
   * @return the configuration built from environment variables
   * @throws IllegalArgumentException if DATAHUB_SERVER is not set
   */
  @Nonnull
  public static DataHubClientConfigV2 fromEnv() {
    String server = System.getenv("DATAHUB_SERVER");
    if (server == null) {
      server = System.getenv("DATAHUB_GMS_URL");
    }

    if (server == null) {
      throw new IllegalArgumentException(
          "DATAHUB_SERVER or DATAHUB_GMS_URL environment variable must be set");
    }

    String token = System.getenv("DATAHUB_TOKEN");
    if (token == null) {
      token = System.getenv("DATAHUB_GMS_TOKEN");
    }

    DataHubClientConfigV2Builder builder = DataHubClientConfigV2.builder().server(server);

    if (token != null) {
      builder.token(token);
    }

    return builder.build();
  }
}
