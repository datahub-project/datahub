package utils;

import com.typesafe.config.Config;

public class KeyStoreConfig {
  public final String path;
  public final String password;
  public final String type;
  public final boolean metadataServiceUseSsl;

  public KeyStoreConfig(String path, String password, String type, boolean metadataServiceUseSsl) {
    this.path = path;
    this.password = password;
    this.type = type;
    this.metadataServiceUseSsl = metadataServiceUseSsl;
  }

  public boolean isValid() {
    return metadataServiceUseSsl && path != null && password != null;
  }

  public static KeyStoreConfig fromConfig(Config config) {
    return new KeyStoreConfig(
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_KEY_STORE_PATH, null),
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_KEY_STORE_PASSWORD, null),
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_KEY_STORE_TYPE, "PKCS12"),
        ConfigUtil.getBoolean(
            config,
            ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL));
  }
}
