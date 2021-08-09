package react.auth;

import static react.auth.ConfigUtil.*;


/**
 * Class responsible for extracting and validating OIDC related configurations.
 */
public class OidcConfigs extends SsoConfigs {

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
    public static final String OIDC_CLIENT_AUTHENTICATION_METHOD_CONFIG_PATH = "auth.oidc.clientAuthenticationMethod";
    public static final String OIDC_JIT_USER_PROVISIONING_ENABLED_CONFIG_PATH = "auth.oidc.jitUserProvisioningEnabled";
    public static final String OIDC_EXTRACT_GROUPS_ENABLED = "auth.oidc.extractGroupsEnabled";
    public static final String OIDC_GROUPS_CLAIM_CONFIG_PATH_CONFIG_PATH = "auth.oidc.groupsClaim"; // Claim expected to be an array of group names.

    /**
     * Default values
     */
    private static final String DEFAULT_OIDC_USERNAME_CLAIM = "preferred_username";
    private static final String DEFAULT_OIDC_USERNAME_CLAIM_REGEX = "(.*)";
    private static final String DEFAULT_OIDC_SCOPE = "openid profile email";
    private static final String DEFAULT_OIDC_CLIENT_NAME = "oidc";
    private static final String DEFAULT_OIDC_CLIENT_AUTHENTICATION_METHOD = "client_secret_basic";
    private static final String DEFAULT_OIDC_JIT_USER_PROVISIONING_ENABLED = "true";
    private static final String DEFAULT_OIDC_EXTRACT_GROUPS_ENABLED = "true";
    private static final String DEFAULT_OIDC_GROUPS_CLAIM = "groups";

    private String _clientId;
    private String _clientSecret;
    private String _discoveryUri;
    private String _userNameClaim;
    private String _userNameClaimRegex;
    private String _scope;
    private String _clientName;
    private String _clientAuthenticationMethod;
    private boolean _jitProvisioningEnabled;
    private boolean _extractGroupsEnabled;
    private String _groupsClaimName;

    public OidcConfigs(final com.typesafe.config.Config configs) {
        super(configs);
        _clientId = getRequired(configs, OIDC_CLIENT_ID_CONFIG_PATH);
        _clientSecret = getRequired(configs, OIDC_CLIENT_SECRET_CONFIG_PATH);
        _discoveryUri = getRequired(configs, OIDC_DISCOVERY_URI_CONFIG_PATH);
        _userNameClaim = getOptional(configs, OIDC_USERNAME_CLAIM_CONFIG_PATH, DEFAULT_OIDC_USERNAME_CLAIM);
        _userNameClaimRegex =
            getOptional(configs, OIDC_USERNAME_CLAIM_REGEX_CONFIG_PATH, DEFAULT_OIDC_USERNAME_CLAIM_REGEX);
        _scope = getOptional(configs, OIDC_SCOPE_CONFIG_PATH, DEFAULT_OIDC_SCOPE);
        _clientName = getOptional(configs, OIDC_CLIENT_NAME_CONFIG_PATH, DEFAULT_OIDC_CLIENT_NAME);
        _clientAuthenticationMethod = getOptional(configs, OIDC_CLIENT_AUTHENTICATION_METHOD_CONFIG_PATH,
            DEFAULT_OIDC_CLIENT_AUTHENTICATION_METHOD);
        _jitProvisioningEnabled = Boolean.parseBoolean(
            getOptional(configs, OIDC_JIT_USER_PROVISIONING_ENABLED_CONFIG_PATH,
                DEFAULT_OIDC_JIT_USER_PROVISIONING_ENABLED));
        _extractGroupsEnabled = Boolean.parseBoolean(
            getOptional(configs, OIDC_EXTRACT_GROUPS_ENABLED, DEFAULT_OIDC_EXTRACT_GROUPS_ENABLED));
        _groupsClaimName = getOptional(configs, OIDC_GROUPS_CLAIM_CONFIG_PATH_CONFIG_PATH, DEFAULT_OIDC_GROUPS_CLAIM);
    }

    public boolean isJitUserProvisioningEnabled() {
        return _jitProvisioningEnabled;
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

    public boolean isExtractGroupsEnabled() {
        return _extractGroupsEnabled;
    }

    public String groupsClaimName() {
        return _groupsClaimName;
    }

    public String getClientAuthenticationMethod() {
        return _clientAuthenticationMethod;
    }
}
