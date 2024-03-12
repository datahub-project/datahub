package auth.sso;

import static auth.ConfigUtil.*;

/** Class responsible for extracting and validating top-level SSO related configurations. */
public class SsoConfigs {

  /** Required configs */
  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";

  private static final String AUTH_BASE_CALLBACK_PATH_CONFIG_PATH = "auth.baseCallbackPath";
  private static final String AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH = "auth.successRedirectPath";
  public static final String OIDC_ENABLED_CONFIG_PATH = "auth.oidc.enabled";

  /** Default values */
  private static final String DEFAULT_BASE_CALLBACK_PATH = "/callback";

  private static final String DEFAULT_SUCCESS_REDIRECT_PATH = "/";

  private final String _authBaseUrl;
  private final String _authBaseCallbackPath;
  private final String _authSuccessRedirectPath;
  private final Boolean _oidcEnabled;

  public SsoConfigs(final com.typesafe.config.Config configs) {
    _authBaseUrl = getRequired(configs, AUTH_BASE_URL_CONFIG_PATH);
    _authBaseCallbackPath =
        getOptional(configs, AUTH_BASE_CALLBACK_PATH_CONFIG_PATH, DEFAULT_BASE_CALLBACK_PATH);
    _authSuccessRedirectPath =
        getOptional(configs, AUTH_SUCCESS_REDIRECT_PATH_CONFIG_PATH, DEFAULT_SUCCESS_REDIRECT_PATH);
    _oidcEnabled =
        configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
            && Boolean.TRUE.equals(
                Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)));
  }

  public String getAuthBaseUrl() {
    return _authBaseUrl;
  }

  public String getAuthBaseCallbackPath() {
    return _authBaseCallbackPath;
  }

  public String getAuthSuccessRedirectPath() {
    return _authSuccessRedirectPath;
  }

  public Boolean isOidcEnabled() {
    return _oidcEnabled;
  }
}
