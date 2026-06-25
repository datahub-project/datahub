package auth;

import static auth.ConfigUtil.*;

/**
 * Configuration properties for users authenticating from a trusted reverse proxy.
 */
public class ProxyAuthConfigs {
  public static final String PROXY_ENABLED_CONFIG_PATH = "auth.proxy.enabled";
  public static final String PROXY_USER_HEADER_CONFIG_PATH = "auth.proxy.userHeader";
  public static final String PROXY_JIT_PROVISIONING_ENABLED_CONFIG_PATH =
      "auth.proxy.jitProvisioningEnabled";

  private static final String DEFAULT_USER_HEADER = "X-Forwarded-User";
  private static final String DEFAULT_JIT_PROVISIONING_ENABLED = "true";

  private final boolean enabled;
  private final String userHeader;
  private final boolean jitProvisioningEnabled;

  public ProxyAuthConfigs(final com.typesafe.config.Config configs) {
    enabled =
        configs.hasPath(PROXY_ENABLED_CONFIG_PATH) && configs.getBoolean(PROXY_ENABLED_CONFIG_PATH);
    userHeader = getOptional(configs, PROXY_USER_HEADER_CONFIG_PATH, DEFAULT_USER_HEADER);
    jitProvisioningEnabled =
        Boolean.parseBoolean(
            getOptional(
                configs,
                PROXY_JIT_PROVISIONING_ENABLED_CONFIG_PATH,
                DEFAULT_JIT_PROVISIONING_ENABLED));
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getUserHeader() {
    return userHeader;
  }

  public boolean isJitProvisioningEnabled() {
    return jitProvisioningEnabled;
  }
}
