package auth;

/**
 * Currently, this config enables or disable custom Java Authentication and Authorization Service
 * authentication that has traditionally existed in DH.
 */
public class JAASConfigs {

  public static final String JAAS_ENABLED_CONFIG_PATH = "auth.jaas.enabled";

  private Boolean _isEnabled = true;

  public JAASConfigs(final com.typesafe.config.Config configs) {
    if (configs.hasPath(JAAS_ENABLED_CONFIG_PATH)
        && !configs.getBoolean(JAAS_ENABLED_CONFIG_PATH)) {
      _isEnabled = false;
    }
  }

  public boolean isJAASEnabled() {
    return _isEnabled;
  }
}
