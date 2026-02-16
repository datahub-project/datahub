package utils;

import com.linkedin.util.Configuration;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigUtil {

  private ConfigUtil() {}

  // New configurations, provided via application.conf file.
  public static final String METADATA_SERVICE_HOST_CONFIG_PATH = "metadataService.host";
  public static final String METADATA_SERVICE_PORT_CONFIG_PATH = "metadataService.port";
  public static final String METADATA_SERVICE_BASE_PATH_CONFIG_PATH = "metadataService.basePath";
  public static final String METADATA_SERVICE_BASE_PATH_ENABLED_CONFIG_PATH =
      "metadataService.basePathEnabled";
  public static final String METADATA_SERVICE_USE_SSL_CONFIG_PATH = "metadataService.useSsl";
  public static final String METADATA_SERVICE_SSL_PROTOCOL_CONFIG_PATH =
      "metadataService.sslProtocol";

  /** TLS protocols for the frontend HTTP client (outbound to GMS/IdP), not server-side. */
  public static final String METADATA_SERVICE_CLIENT_SSL_ENABLED_PROTOCOLS_CONFIG_PATH =
      "metadataService.clientSslEnabledProtocols";

  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PATH =
      "metadataService.truststore.path";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PASSWORD =
      "metadataService.truststore.password";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_TYPE =
      "metadataService.truststore.type";

  // Legacy env-var based config values, for backwards compatibility:
  public static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  public static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
  public static final String GMS_BASE_PATH_ENV_VAR = "DATAHUB_GMS_BASE_PATH";
  public static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
  public static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";
  public static final String GMS_CLIENT_SSL_ENABLED_PROTOCOLS_VAR =
      "DATAHUB_GMS_CLIENT_SSL_ENABLED_PROTOCOLS";

  // Default values
  public static final String DEFAULT_GMS_HOST = "localhost";
  public static final String DEFAULT_GMS_PORT = "8080";
  public static final String DEFAULT_GMS_BASE_PATH = "";
  public static final String DEFAULT_GMS_BASE_PATH_ENABLED = "false";
  public static final String DEFAULT_GMS_USE_SSL = "False";

  public static final String DEFAULT_METADATA_SERVICE_HOST =
      Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");
  public static final Integer DEFAULT_METADATA_SERVICE_PORT =
      Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));
  public static final String DEFAULT_METADATA_SERVICE_BASE_PATH = "";
  public static final Boolean DEFAULT_METADATA_SERVICE_BASE_PATH_ENABLED = false;
  public static final Boolean DEFAULT_METADATA_SERVICE_USE_SSL =
      Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));
  public static final String DEFAULT_METADATA_SERVICE_SSL_PROTOCOL =
      Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);

  /** Default TLS protocols for the frontend client (outbound to GMS/IdP); TLS 1.0/1.1 disabled. */
  public static final List<String> DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS =
      Collections.unmodifiableList(Arrays.asList("TLSv1.2", "TLSv1.3"));

  public static boolean getBoolean(Config config, String key) {
    return config.hasPath(key) && config.getBoolean(key);
  }

  public static boolean getBoolean(Config config, String key, boolean defaultValue) {
    return config.hasPath(key) ? config.getBoolean(key) : defaultValue;
  }

  public static int getInt(Config config, String key, int defaultValue) {
    return config.hasPath(key) ? config.getInt(key) : defaultValue;
  }

  public static String getString(Config config, String key, String defaultValue) {
    return config.hasPath(key) ? config.getString(key) : defaultValue;
  }

  /**
   * Returns a list of strings from a comma-delimited config value (e.g. from env var). Uses {@link
   * #getString(Config, String, String)}; blank or missing returns defaultValue.
   */
  public static List<String> getStringList(Config config, String key, List<String> defaultValue) {
    String s = getString(config, key, null);
    if (s == null || s.isBlank()) {
      return defaultValue;
    }
    return Arrays.stream(s.split(","))
        .map(String::trim)
        .filter(p -> !p.isEmpty())
        .collect(Collectors.toList());
  }
}
