package utils;

import com.typesafe.config.Config;
import java.util.List;

public class TruststoreConfig {
  public final String path;
  public final String password;
  public final String type;
  public final boolean metadataServiceUseSsl;
  public final List<String> sslEnabledProtocols;

  public TruststoreConfig(
      String path,
      String password,
      String type,
      boolean metadataServiceUseSsl,
      List<String> sslEnabledProtocols) {
    this.path = path;
    this.password = password;
    this.type = type;
    this.metadataServiceUseSsl = metadataServiceUseSsl;
    this.sslEnabledProtocols =
        sslEnabledProtocols != null && !sslEnabledProtocols.isEmpty()
            ? sslEnabledProtocols
            : ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS;
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
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL),
        ConfigUtil.getStringList(
            config,
            ConfigUtil.METADATA_SERVICE_CLIENT_SSL_ENABLED_PROTOCOLS_CONFIG_PATH,
            ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS));
  }
}
