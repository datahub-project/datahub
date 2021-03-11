package react.auth;

/**
 * Class responsible for extracting and validating OIDC related configurations.
 */
public class OidcConfigs {

    public static final String OIDC_ENABLED_CONFIG_PATH = "auth.oidc.enabled";

    /**
     * Required configs
     */
    public static final String OIDC_CLIENT_ID_CONFIG_PATH = "auth.oidc.clientId";
    public static final String OIDC_CLIENT_SECRET_CONFIG_PATH = "auth.oidc.clientSecret";
    public static final String OIDC_DISCOVERY_URI_CONFIG_PATH = "auth.oidc.discoveryUri";

    /**
     * Optional configs
     */
    public static final String OIDC_USERNAME_CLAIM_CONFIG_PATH = "auth.oidc.userNameClaim";
    public static final String OIDC_USERNAME_CLAIM_REGEX_CONFIG_PATH = "auth.oidc.userNameClaimRegex";
    public static final String OIDC_SCOPE_CONFIG_PATH = "auth.oidc.scope";
    public static final String OIDC_CLIENT_NAME_CONFIG_PATH = "auth.oidc.clientName";

    /**
     * Default values
     */
    private static final String DEFAULT_OIDC_USERNAME_CLAIM = "preferred_username";
    private static final String DEFAULT_OIDC_USERNAME_CLAIM_REGEX = "(.*)";
    private static final String DEFAULT_OIDC_SCOPE = "openid profile email";
    private static final String DEFAULT_OIDC_CLIENT_NAME = "oidc";

    private String _clientId;
    private String _clientSecret;
    private String _discoveryUri;
    private String _userNameClaim;
    private String _userNameClaimRegex;
    private String _scope;
    private String _clientName;

    private Boolean _isEnabled = false;

    public OidcConfigs(final com.typesafe.config.Config configs) {
        if (configs.hasPath(OIDC_ENABLED_CONFIG_PATH)
                && Boolean.TRUE.equals(
                        Boolean.parseBoolean(configs.getString(OIDC_ENABLED_CONFIG_PATH)))) {
            _isEnabled = true;
            _clientId = getRequired(
                    configs,
                    OIDC_CLIENT_ID_CONFIG_PATH);
            _clientSecret = getRequired(
                    configs,
                    OIDC_CLIENT_SECRET_CONFIG_PATH);
            _discoveryUri = getRequired(
                    configs,
                    OIDC_DISCOVERY_URI_CONFIG_PATH);
            _userNameClaim = getOptional(
                    configs,
                    OIDC_USERNAME_CLAIM_CONFIG_PATH,
                    DEFAULT_OIDC_USERNAME_CLAIM);
            _userNameClaimRegex = getOptional(
                    configs,
                    OIDC_USERNAME_CLAIM_REGEX_CONFIG_PATH,
                    DEFAULT_OIDC_USERNAME_CLAIM_REGEX);
            _scope = getOptional(
                    configs,
                    OIDC_SCOPE_CONFIG_PATH,
                    DEFAULT_OIDC_SCOPE);
            _clientName = getOptional(
                    configs,
                    OIDC_CLIENT_NAME_CONFIG_PATH,
                    DEFAULT_OIDC_CLIENT_NAME);
        }
    }

    public boolean isOidcEnabled() {
        return _isEnabled;
    }

    public String getClientId() {
        return _clientId;
    }

    public String getClientSecret() {
        return _clientSecret;
    }

    public String getDiscoveryUri() {
        return _discoveryUri;
    }

    public String getUserNameClaim() {
        return _userNameClaim;
    }

    public String getUserNameClaimRegex() {
        return _userNameClaimRegex;
    }

    public String getScope() {
        return _scope;
    }

    public String getClientName() {
        return _clientName;
    }

    private String getRequired(final com.typesafe.config.Config configs, final String path) {
        if (!configs.hasPath(path)) {
            throw new IllegalArgumentException(
                    String.format("Missing required OIDC config with path %s", path));
        }
        return configs.getString(path);
    }

    private String getOptional(final com.typesafe.config.Config configs,
                               final String path,
                               final String defaultVal) {
        if (!configs.hasPath(path)) {
            return defaultVal;
        }
        return configs.getString(path);
    }
}
