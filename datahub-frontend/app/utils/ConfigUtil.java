package utils;

import com.linkedin.util.Configuration;
import com.typesafe.config.Config;
import io.datahubproject.metadata.services.SecretService;

public class ConfigUtil {

  private static final SecretService secretService;

  static {
    // Initialize SecretService once at class load
    String base64Key = System.getenv("SECRET_SERVICE_ENCRYPTION_KEY");
    if (base64Key != null && !base64Key.isEmpty()) {
      secretService = new SecretService(base64Key);
    } else {
      secretService = null; // no encryption key provided
    }
  }

  private ConfigUtil() {}

  // Config paths
  public static final String METADATA_SERVICE_HOST_CONFIG_PATH = "metadataService.host";
  public static final String METADATA_SERVICE_PORT_CONFIG_PATH = "metadataService.port";
  public static final String METADATA_SERVICE_BASE_PATH_CONFIG_PATH = "metadataService.basePath";
  public static final String METADATA_SERVICE_BASE_PATH_ENABLED_CONFIG_PATH =
      "metadataService.basePathEnabled";
  public static final String METADATA_SERVICE_USE_SSL_CONFIG_PATH = "metadataService.useSsl";
  public static final String METADATA_SERVICE_SSL_PROTOCOL_CONFIG_PATH =
      "metadataService.sslProtocol";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PATH =
      "metadataService.truststore.path";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PASSWORD =
      "metadataService.truststore.password";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_TYPE =
      "metadataService.truststore.type";

  // Legacy env vars
  public static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  public static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
  public static final String GMS_BASE_PATH_ENV_VAR = "DATAHUB_GMS_BASE_PATH";
  public static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
  public static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

  // Defaults
  public static final String DEFAULT_GMS_HOST = "localhost";
  public static final String DEFAULT_GMS_PORT = "8080";
  public static final String DEFAULT_GMS_BASE_PATH = "";
  public static final String DEFAULT_GMS_BASE_PATH_ENABLED = "false";
  public static final String DEFAULT_GMS_USE_SSL = "False";

  public static final String DEFAULT_METADATA_SERVICE_HOST =
      Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, DEFAULT_GMS_HOST);
  public static final Integer DEFAULT_METADATA_SERVICE_PORT =
      Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, DEFAULT_GMS_PORT));
  public static final String DEFAULT_METADATA_SERVICE_BASE_PATH = "";
  public static final Boolean DEFAULT_METADATA_SERVICE_BASE_PATH_ENABLED = false;
  public static final Boolean DEFAULT_METADATA_SERVICE_USE_SSL =
      Boolean.parseBoolean(
          Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, DEFAULT_GMS_USE_SSL));
  public static final String DEFAULT_METADATA_SERVICE_SSL_PROTOCOL =
      Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);

  /** Decrypt only if encryption key and SecretService are available. */
  private static String decryptIfNeeded(String value) {
    if (value == null) {
      return null;
    }
    try {
      if (secretService != null && isEncrypted(value)) {
        String cipherText = value.substring(4, value.length() - 1);
        return secretService.decrypt(cipherText);
      }
      return value; // plain text
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt config value: " + value, e);
    }
  }

  /** Example check — adjust based on your encryption format. */
  private static boolean isEncrypted(String value) {
    // Example: values stored as ENC(<ciphertext>)
    return value.startsWith("ENC(") && value.endsWith(")");
  }

  public static boolean getBoolean(Config config, String key, boolean defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return Boolean.parseBoolean(decryptIfNeeded(config.getString(key)));
  }

  public static int getInt(Config config, String key, int defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return Integer.parseInt(decryptIfNeeded(config.getString(key)));
  }

  public static String getString(Config config, String key, String defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return decryptIfNeeded(config.getString(key));
  }
}
