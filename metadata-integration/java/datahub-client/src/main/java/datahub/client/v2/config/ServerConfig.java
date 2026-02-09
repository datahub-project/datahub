package datahub.client.v2.config;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents DataHub server configuration fetched from the /config endpoint.
 *
 * <p>Provides version detection, feature support checking, and deployment type (Cloud vs Core)
 * identification.
 *
 * <p>Example server config JSON:
 *
 * <pre>{@code
 * {
 *   "versions": {
 *     "acryldata/datahub": {
 *       "version": "v1.0.0",
 *       "commit": "abc123"
 *     }
 *   },
 *   "datahub": {
 *     "serverType": "prod",
 *     "serverEnv": "core"
 *   },
 *   "patchCapable": true,
 *   "statefulIngestionCapable": true
 * }
 * }</pre>
 */
public class ServerConfig {
  private final JsonNode rawConfig;
  private final Version version;
  private final String serverEnv;
  private final long fetchTimestamp;

  /**
   * Constructs a ServerConfig.
   *
   * @param rawConfig raw JSON configuration from /config endpoint
   * @param version parsed version
   * @param serverEnv server environment ("core" or other for cloud)
   * @param fetchTimestamp timestamp when config was fetched
   */
  public ServerConfig(
      @Nonnull JsonNode rawConfig,
      @Nonnull Version version,
      @Nullable String serverEnv,
      long fetchTimestamp) {
    this.rawConfig = rawConfig;
    this.version = version;
    this.serverEnv = serverEnv;
    this.fetchTimestamp = fetchTimestamp;
  }

  /**
   * Parses server config from JSON response.
   *
   * @param configJson JSON from /config endpoint
   * @return parsed ServerConfig
   */
  @Nonnull
  public static ServerConfig fromJson(@Nonnull JsonNode configJson) {
    // Extract version string
    String versionString = null;
    JsonNode versionsNode = configJson.get("versions");
    if (versionsNode != null && versionsNode.has("acryldata/datahub")) {
      JsonNode datahubVersion = versionsNode.get("acryldata/datahub");
      if (datahubVersion.has("version")) {
        versionString = datahubVersion.get("version").asText();
      }
    }

    // Parse version (returns 0.0.0.0 if null)
    Version version = Version.parse(versionString);

    // Extract server environment
    String serverEnv = null;
    JsonNode datahubNode = configJson.get("datahub");
    if (datahubNode != null && datahubNode.has("serverEnv")) {
      serverEnv = datahubNode.get("serverEnv").asText();
    }

    return new ServerConfig(configJson, version, serverEnv, System.currentTimeMillis());
  }

  /**
   * Gets the parsed version.
   *
   * @return server version
   */
  @Nonnull
  public Version getVersion() {
    return version;
  }

  /**
   * Gets the raw config JSON.
   *
   * @return raw config
   */
  @Nonnull
  public JsonNode getRawConfig() {
    return rawConfig;
  }

  /**
   * Gets the server environment string.
   *
   * @return server environment (e.g., "core", or null for cloud)
   */
  @Nullable
  public String getServerEnv() {
    return serverEnv;
  }

  /**
   * Gets the timestamp when this config was fetched.
   *
   * @return fetch timestamp in milliseconds
   */
  public long getFetchTimestamp() {
    return fetchTimestamp;
  }

  /**
   * Checks if this is DataHub Cloud deployment.
   *
   * @return true if cloud, false if self-hosted core
   */
  public boolean isCloud() {
    return serverEnv == null || !serverEnv.equals("core");
  }

  /**
   * Checks if this is DataHub Core (self-hosted) deployment.
   *
   * @return true if core, false if cloud
   */
  public boolean isCore() {
    return !isCloud();
  }

  /**
   * Checks if server version is at least the specified version.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true if server version >= specified version
   */
  public boolean isVersionAtLeast(int major, int minor, int patch) {
    return version.isAtLeast(major, minor, patch);
  }

  /**
   * Checks if server version is at least the specified version.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @param build build number
   * @return true if server version >= specified version
   */
  public boolean isVersionAtLeast(int major, int minor, int patch, int build) {
    return version.isAtLeast(major, minor, patch, build);
  }

  /**
   * Checks if server supports a specific feature.
   *
   * <p>Feature support is determined by:
   *
   * <ul>
   *   <li>Config flags (e.g., patchCapable) for config-based features
   *   <li>Version thresholds for version-based features
   * </ul>
   *
   * @param feature feature to check
   * @return true if feature is supported
   */
  public boolean supportsFeature(@Nonnull ServerFeature feature) {
    switch (feature) {
      case PATCH_CAPABLE:
        return rawConfig.has("patchCapable") && rawConfig.get("patchCapable").asBoolean(false);

      case STATEFUL_INGESTION:
        return rawConfig.has("statefulIngestionCapable")
            && rawConfig.get("statefulIngestionCapable").asBoolean(false);

      case IMPACT_ANALYSIS:
        return rawConfig.has("supportsImpactAnalysis")
            && rawConfig.get("supportsImpactAnalysis").asBoolean(false);

      case DATAHUB_CLOUD:
        return isCloud();

      case OPENAPI_SDK:
      case API_TRACING:
        // Version-based features with different thresholds for cloud vs core
        if (isCloud()) {
          // Cloud requires 0.3.11+
          return isVersionAtLeast(0, 3, 11, 0);
        } else {
          // Core requires 1.0.1+
          return isVersionAtLeast(1, 0, 1, 0);
        }

      default:
        return false;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "ServerConfig{version=%s, serverEnv=%s, isCloud=%s}", version, serverEnv, isCloud());
  }
}
