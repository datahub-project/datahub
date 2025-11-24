package utils;

import com.linkedin.util.Configuration;
import com.typesafe.config.Config;

public class ConfigUtil {

  private static final String ALGO = "AES";

  private ConfigUtil() {}

  // New configurations, provided via application.conf file.
  public static final String METADATA_SERVICE_HOST_CONFIG_PATH = "metadataService.host";
  public static final String METADATA_SERVICE_PORT_CONFIG_PATH = "metadataService.port";
  public static final String METADATA_SERVICE_USE_SSL_CONFIG_PATH = "metadataService.useSsl";
  public static final String METADATA_SERVICE_SSL_PROTOCOL_CONFIG_PATH =
      "metadataService.sslProtocol";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PATH =
      "metadataService.truststore.path";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_PASSWORD =
      "metadataService.truststore.password";
  public static final String METADATA_SERVICE_SSL_TRUST_STORE_TYPE =
      "metadataService.truststore.type";

  // Legacy env-var based config values, for backwards compatibility:
  public static final String GMS_HOST_ENV_VAR = "DATAHUB_GMS_HOST";
  public static final String GMS_PORT_ENV_VAR = "DATAHUB_GMS_PORT";
  public static final String GMS_USE_SSL_ENV_VAR = "DATAHUB_GMS_USE_SSL";
  public static final String GMS_SSL_PROTOCOL_VAR = "DATAHUB_GMS_SSL_PROTOCOL";

  // Default values
  public static final String DEFAULT_GMS_HOST = "localhost";
  public static final String DEFAULT_GMS_PORT = "8080";
  public static final String DEFAULT_GMS_USE_SSL = "False";

  public static final String DEFAULT_METADATA_SERVICE_HOST =
      Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR, "localhost");
  public static final Integer DEFAULT_METADATA_SERVICE_PORT =
      Integer.parseInt(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR, "8080"));
  public static final Boolean DEFAULT_METADATA_SERVICE_USE_SSL =
      Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False"));
  public static final String DEFAULT_METADATA_SERVICE_SSL_PROTOCOL =
      Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR);

  private static String decrypt(String value) {
    if (value == null) {
      return null;
    }

    if (!value.startsWith("ENC(") && !value.endsWith(")")) {
      return value;
    }

    try {
      String base64Key = System.getenv("CONFIG_ENCRYPTION_KEY");
      if (base64Key == null || base64Key.isEmpty()) {
        throw new IllegalStateException("CONFIG_ENCRYPTION_KEY is required for encrypted config");
      }

      String cipherText = value.substring(4, value.length() - 1);

      byte[] keyBytes = java.util.Base64.getDecoder().decode(base64Key);
      javax.crypto.spec.SecretKeySpec keySpec = new javax.crypto.spec.SecretKeySpec(keyBytes, ALGO);

      javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(ALGO);
      cipher.init(javax.crypto.Cipher.DECRYPT_MODE, keySpec);

      byte[] decrypted = cipher.doFinal(java.util.Base64.getDecoder().decode(cipherText));

      return new String(decrypted);

    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt config value: " + value, e);
    }
  }

  public static boolean getBoolean(Config config, String key, boolean defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return Boolean.parseBoolean(decrypt(config.getString(key)));
  }

  public static int getInt(Config config, String key, int defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return Integer.parseInt(decrypt(config.getString(key)));
  }

  public static String getString(Config config, String key, String defaultValue) {
    if (!config.hasPath(key)) {
      return defaultValue;
    }
    return decrypt(config.getString(key));
  }
}
