package datahub.client.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.client.v2.config.DataHubClientConfigV2;
import datahub.client.v2.config.EmitMode;
import datahub.client.v2.config.ServerConfig;
import datahub.client.v2.operations.EntityClient;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Main entry point for DataHub Java SDK V2.
 *
 * <p>Provides high-level operations for entity management with a fluent, type-safe API.
 *
 * <p>Example usage:
 *
 * <pre>
 * // Create client
 * DataHubClientV2 client = DataHubClientV2.builder()
 *     .server("http://localhost:8080")
 *     .token("my-token")
 *     .build();
 *
 * // Create a dataset
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .description("My dataset")
 *     .build();
 *
 * // Add metadata
 * dataset.addTag("pii");
 * dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.DATA_OWNER);
 * dataset.addTerm("urn:li:glossaryTerm:CustomerData");
 *
 * // Upsert to DataHub
 * client.entities().upsert(dataset);
 *
 * // Close when done
 * client.close();
 * </pre>
 *
 * <p>For patch operations:
 *
 * <pre>
 * DatasetPatch patch = DatasetPatch.builder()
 *     .urn(datasetUrn)
 *     .addTag("sensitive")
 *     .addTerm("urn:li:glossaryTerm:FinancialData")
 *     .build();
 *
 * client.entities().patch(patch);
 * </pre>
 *
 * <p>This class is thread-safe and should be reused across the application. Create once and use for
 * all entity operations.
 */
@Slf4j
public class DataHubClientV2 implements AutoCloseable {

  @Getter @Nonnull private final RestEmitter emitter;
  @Getter @Nonnull private final DataHubClientConfigV2 config;
  @Getter @Nonnull private final EntityClient entityClient;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Nullable private ServerConfig serverConfig;
  @Nullable private Long configFetchTime;

  /**
   * Private constructor. Use {@link #builder()} to create instances.
   *
   * @param config the client configuration
   * @throws IOException if emitter initialization fails
   */
  private DataHubClientV2(@Nonnull DataHubClientConfigV2 config) throws IOException {
    this.config = config;
    this.emitter = new RestEmitter(config.toRestEmitterConfig());
    this.entityClient = new EntityClient(emitter, config, this);
    log.info("DataHub Client V2 initialized for server: {}", config.getServer());
  }

  /**
   * Returns the entity operations client for CRUD operations.
   *
   * @return the entity client
   */
  @Nonnull
  public EntityClient entities() {
    return entityClient;
  }

  /**
   * Tests the connection to DataHub server.
   *
   * @return true if connection is successful
   * @throws IOException if connection test fails
   * @throws ExecutionException if future execution fails
   * @throws InterruptedException if waiting is interrupted
   */
  public boolean testConnection() throws IOException, ExecutionException, InterruptedException {
    log.debug("Testing connection to DataHub server: {}", config.getServer());
    return emitter.testConnection();
  }

  /**
   * Gets the server configuration with lazy loading and TTL-based caching.
   *
   * <p>The server config is fetched on first access and cached according to the
   * configRefreshIntervalMs setting:
   *
   * <ul>
   *   <li>Default (60000ms): Config refreshed after 60 seconds
   *   <li>0ms: No caching, fetches on every call
   *   <li>-1ms: Cache indefinitely, never refresh
   * </ul>
   *
   * @return the server configuration, or null if fetching fails
   */
  @Nullable
  public synchronized ServerConfig getServerConfig() {
    if (shouldRefreshConfig()) {
      try {
        serverConfig = fetchServerConfig();
        configFetchTime = System.currentTimeMillis();
        log.debug("Fetched server config: {}", serverConfig);
      } catch (Exception e) {
        log.warn(
            "Failed to fetch server config from {}/config: {}", config.getServer(), e.getMessage());
        // Return stale config if available, otherwise null
      }
    }
    return serverConfig;
  }

  /** Invalidates the cached server config, forcing a refresh on next access. */
  public synchronized void invalidateConfigCache() {
    log.debug("Invalidating server config cache");
    configFetchTime = null;
  }

  /**
   * Checks if the server config should be refreshed based on TTL settings.
   *
   * @return true if config should be refreshed
   */
  private boolean shouldRefreshConfig() {
    long refreshInterval = config.getConfigRefreshIntervalMs();

    // No caching - always refresh
    if (refreshInterval == 0) {
      return true;
    }

    // Cache indefinitely
    if (refreshInterval == -1 && serverConfig != null) {
      return false;
    }

    // Check if config exists and is fresh
    if (serverConfig != null && configFetchTime != null) {
      long age = System.currentTimeMillis() - configFetchTime;
      return age >= refreshInterval;
    }

    // Config doesn't exist - need to fetch
    return true;
  }

  /**
   * Fetches the server configuration from the /config endpoint.
   *
   * @return the parsed server configuration
   * @throws IOException if fetching or parsing fails
   * @throws ExecutionException if future execution fails
   * @throws InterruptedException if waiting is interrupted
   */
  @Nonnull
  private ServerConfig fetchServerConfig()
      throws IOException, ExecutionException, InterruptedException {
    String url = config.getServer() + "/config";
    log.debug("Fetching server config from: {}", url);

    MetadataWriteResponse response = emitter.get(url).get();

    if (!response.isSuccess()) {
      throw new IOException("Failed to fetch server config from " + url);
    }

    String responseBody = response.getResponseContent();
    if (responseBody == null || responseBody.isEmpty()) {
      throw new IOException("Empty response body from config endpoint");
    }

    JsonNode configJson = objectMapper.readTree(responseBody);
    return ServerConfig.fromJson(configJson);
  }

  /**
   * Closes the client and releases resources.
   *
   * @throws IOException if closing fails
   */
  @Override
  public void close() throws IOException {
    log.info("Closing DataHub Client V2");
    emitter.close();
  }

  /**
   * Creates a new builder for DataHubClientV2.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for DataHubClientV2.
   *
   * <p>Provides a fluent API for configuring and creating client instances.
   */
  public static class Builder {
    private String server;
    private String token;
    private Integer timeoutMs;
    private Boolean disableSslVerification;
    private Integer maxRetries;
    private EmitMode emitMode;
    private DataHubClientConfigV2 config;

    /**
     * Sets the DataHub server URL.
     *
     * @param server the server URL (e.g., "http://localhost:8080")
     * @return this builder
     */
    @Nonnull
    public Builder server(@Nonnull String server) {
      this.server = server;
      return this;
    }

    /**
     * Sets the authentication token.
     *
     * @param token the token
     * @return this builder
     */
    @Nonnull
    public Builder token(@Nonnull String token) {
      this.token = token;
      return this;
    }

    /**
     * Sets the request timeout in milliseconds.
     *
     * @param timeoutMs the timeout in milliseconds
     * @return this builder
     */
    @Nonnull
    public Builder timeoutMs(int timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    /**
     * Disables SSL certificate verification. WARNING: Only use for testing!
     *
     * @param disable true to disable SSL verification
     * @return this builder
     */
    @Nonnull
    public Builder disableSslVerification(boolean disable) {
      this.disableSslVerification = disable;
      return this;
    }

    /**
     * Sets the maximum number of retries for failed requests.
     *
     * @param maxRetries the maximum number of retries
     * @return this builder
     */
    @Nonnull
    public Builder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the emit mode for metadata changes.
     *
     * @param emitMode the emit mode (SYNC_WAIT, SYNC_PRIMARY, or ASYNC)
     * @return this builder
     */
    @Nonnull
    public Builder emitMode(@Nonnull EmitMode emitMode) {
      this.emitMode = emitMode;
      return this;
    }

    /**
     * Sets a pre-built configuration. This overrides any individual settings.
     *
     * @param config the configuration
     * @return this builder
     */
    @Nonnull
    public Builder config(@Nonnull DataHubClientConfigV2 config) {
      this.config = config;
      return this;
    }

    /**
     * Builds the DataHubClientV2 instance.
     *
     * @return the client instance
     * @throws IOException if client initialization fails
     * @throws IllegalArgumentException if required configuration is missing
     */
    @Nonnull
    public DataHubClientV2 build() throws IOException {
      DataHubClientConfigV2 finalConfig;

      if (config != null) {
        finalConfig = config;
      } else {
        if (server == null) {
          throw new IllegalArgumentException("Server URL is required");
        }

        DataHubClientConfigV2.DataHubClientConfigV2Builder configBuilder =
            DataHubClientConfigV2.builder().server(server);

        if (token != null) {
          configBuilder.token(token);
        }
        if (timeoutMs != null) {
          configBuilder.timeoutMs(timeoutMs);
        }
        if (disableSslVerification != null) {
          configBuilder.disableSslVerification(disableSslVerification);
        }
        if (maxRetries != null) {
          configBuilder.maxRetries(maxRetries);
        }
        if (emitMode != null) {
          configBuilder.emitMode(emitMode);
        }

        finalConfig = configBuilder.build();
      }

      return new DataHubClientV2(finalConfig);
    }

    /**
     * Builds a client from environment variables.
     *
     * <p>Required environment variables: - DATAHUB_SERVER or DATAHUB_GMS_URL: Server URL
     *
     * <p>Optional environment variables: - DATAHUB_TOKEN or DATAHUB_GMS_TOKEN: Authentication token
     *
     * @return the client instance
     * @throws IOException if client initialization fails
     * @throws IllegalArgumentException if required environment variables are missing
     */
    @Nonnull
    public DataHubClientV2 buildFromEnv() throws IOException {
      DataHubClientConfigV2 envConfig = DataHubClientConfigV2.fromEnv();
      return new DataHubClientV2(envConfig);
    }
  }
}
