package utils;

import com.linkedin.util.Configuration;
import com.typesafe.config.Config;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class ConfigUtil {

  private static final String ALGO = "AES";
  private static final String CIPHER_ALGO = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128; // bits
  private static final int IV_LENGTH = 12; // bytes

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

  /** Standard AES/GCM decryptor for ENC(...) values. Format: ENC(Base64(IV + Ciphertext + Tag)) */
  public static String decryptGCM(String encValue, String base64Key) throws Exception {
    if (encValue == null) {
      return null;
    }

    if (!encValue.startsWith("ENC(") || !encValue.endsWith(")")) {
      return encValue; // not encrypted
    }

    String cipherTextBase64 = encValue.substring(4, encValue.length() - 1);
    byte[] cipherMessage = Base64.getDecoder().decode(cipherTextBase64);

    if (cipherMessage.length < (IV_LENGTH + 16)) {
      throw new IllegalStateException(
          "Ciphertext too short to be valid AES/GCM. Re-encrypt using AES/GCM with a 12-byte IV.");
    }

    // Extract IV
    byte[] iv = new byte[IV_LENGTH];
    System.arraycopy(cipherMessage, 0, iv, 0, IV_LENGTH);

    // Extract ciphertext + tag
    byte[] cipherText = new byte[cipherMessage.length - IV_LENGTH];
    System.arraycopy(cipherMessage, IV_LENGTH, cipherText, 0, cipherText.length);

    // Init AES/GCM cipher
    SecretKeySpec keySpec = new SecretKeySpec(Base64.getDecoder().decode(base64Key), ALGO);
    Cipher cipher = Cipher.getInstance(CIPHER_ALGO);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
    cipher.init(Cipher.DECRYPT_MODE, keySpec, spec);

    byte[] decryptedBytes = cipher.doFinal(cipherText);
    return new String(decryptedBytes);
  }

  /** Internal wrapper for config decryption — reads key from ENV. */
  private static String decrypt(String value) {
    try {
      String base64Key = System.getenv("CONFIG_ENCRYPTION_KEY");
      if (base64Key == null || base64Key.isEmpty()) {
        throw new IllegalStateException("CONFIG_ENCRYPTION_KEY is required for encrypted config");
      }
      return decryptGCM(value, base64Key);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt config value (GCM only): " + value, e);
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
