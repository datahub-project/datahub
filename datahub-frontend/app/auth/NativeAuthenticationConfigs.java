package auth;

/**
 * Currently, this config enables or disable native user authentication.
 */
public class NativeAuthenticationConfigs {

  public static final String NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH = "auth.native.enabled";

  private Boolean _isEnabled = true;

  public NativeAuthenticationConfigs(final com.typesafe.config.Config configs) {
    if (configs.hasPath(NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH)
        && Boolean.FALSE.equals(
        Boolean.parseBoolean(configs.getValue(NATIVE_AUTHENTICATION_ENABLED_CONFIG_PATH).toString()))) {
      _isEnabled = false;
    }
  }

  public boolean isNativeAuthenticationEnabled() {
    return _isEnabled;
  }
}
