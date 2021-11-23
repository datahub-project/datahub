package utils;

import com.linkedin.util.Configuration;
import com.typesafe.config.Config;


public class ConfigUtil {

  // New configurations, provided via application.conf file.
  public static final String METADATA_SERVICE_HOST_CONFIG_PATH = "metadataService.host";
  public static final String METADATA_SERVICE_PORT_CONFIG_PATH = "metadataService.port";
  public static final String METADATA_SERVICE_USE_SSL_CONFIG_PATH = "metadataService.useSsl";
  public static final String METADATA_SERVICE_SSL_PROTOCOL_CONFIG_PATH = "metadataService.sslProtocol";

  // Legacy env-var based config values, for backwards compatibility:
  public static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  public static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
  public static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
  public static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

  // Default values
  public static final String DEFAULT_GMS_HOST = "localhost";
  public static final String DEFAULT_GMS_PORT = "8080";
  public static final String DEFAULT_GMS_USE_SSL = "False";

  public static final String DEFAULT_METADATA_SERVICE_HOST = Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");
  public static final Integer DEFAULT_METADATA_SERVICE_PORT = Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));
  public static final Boolean DEFAULT_METADATA_SERVICE_USE_SSL = Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));
  public static final String DEFAULT_METADATA_SERVICE_SSL_PROTOCOL = Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);

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
}
