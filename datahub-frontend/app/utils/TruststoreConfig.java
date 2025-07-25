package utils;

import com.typesafe.config.Config;

public class TruststoreConfig {
  public final String path;
  public final String password;
  public final String type;
  public final boolean metadataServiceUseSsl;

  public TruststoreConfig(
      String path, String password, String type, boolean metadataServiceUseSsl) {
    this.path = path;
    this.password = password;
    this.type = type;
    this.metadataServiceUseSsl = metadataServiceUseSsl;
  }

  public boolean isValid() {
    return metadataServiceUseSsl && path != null && password != null;
  }

  public static TruststoreConfig fromConfig(Config config) {
    return new TruststoreConfig(
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_TRUST_STORE_PATH, null),
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_TRUST_STORE_PASSWORD, null),
        ConfigUtil.getString(config, ConfigUtil.METADATA_SERVICE_SSL_TRUST_STORE_TYPE, "PKCS12"),
        ConfigUtil.getBoolean(
            config,
            ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL));
  }
}
