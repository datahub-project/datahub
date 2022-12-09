package auth.sso.oidc;

import auth.sso.SsoConfigs;
import java.util.Optional;
import lombok.Getter;

import static auth.ConfigUtil.*;


/**
 * Class responsible for extracting and validating OIDC related configurations.
 */
@Getter
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
    public static final String OIDC_JIT_PROVISIONING_ENABLED_CONFIG_PATH = "auth.oidc.jitProvisioningEnabled";
    public static final String OIDC_PRE_PROVISIONING_REQUIRED_CONFIG_PATH = "auth.oidc.preProvisioningRequired";
    public static final String OIDC_EXTRACT_GROUPS_ENABLED = "auth.oidc.extractGroupsEnabled";
    public static final String OIDC_GROUPS_CLAIM_CONFIG_PATH_CONFIG_PATH = "auth.oidc.groupsClaim"; // Claim expected to be an array of group names.
    public static final String OIDC_RESPONSE_TYPE = "auth.oidc.responseType";
    public static final String OIDC_RESPONSE_MODE = "auth.oidc.responseMode";
    public static final String OIDC_USE_NONCE = "auth.oidc.useNonce";
    public static final String OIDC_CUSTOM_PARAM_RESOURCE = "auth.oidc.customParam.resource";
    public static final String OIDC_READ_TIMEOUT = "auth.oidc.readTimeout";
    public static final String OIDC_EXTRACT_JWT_ACCESS_TOKEN_CLAIMS = "auth.oidc.extractJwtAccessTokenClaims";

    /**
     * Default values
     */
    private static final String DEFAULT_OIDC_USERNAME_CLAIM = "email";
    private static final String DEFAULT_OIDC_USERNAME_CLAIM_REGEX = "(.*)";
    private static final String DEFAULT_OIDC_SCOPE = "openid profile email"; // Often "group" must be included for groups.
    private static final String DEFAULT_OIDC_CLIENT_NAME = "oidc";
    private static final String DEFAULT_OIDC_CLIENT_AUTHENTICATION_METHOD = "client_secret_basic";
    private static final String DEFAULT_OIDC_JIT_PROVISIONING_ENABLED = "true";
    private static final String DEFAULT_OIDC_PRE_PROVISIONING_REQUIRED = "false";
    private static final String DEFAULT_OIDC_EXTRACT_GROUPS_ENABLED = "false"; // False since extraction of groups can overwrite existing group membership.
    private static final String DEFAULT_OIDC_GROUPS_CLAIM = "groups";
    private static final String DEFAULT_OIDC_READ_TIMEOUT = "5000";

    private String clientId;
    private String clientSecret;
    private String discoveryUri;
    private String userNameClaim;
    private String userNameClaimRegex;
    private String scope;
    private String clientName;
    private String clientAuthenticationMethod;
    private boolean jitProvisioningEnabled;
    private boolean preProvisioningRequired;
    private boolean extractGroupsEnabled;
    private String groupsClaimName;
    private Optional<String> responseType;
    private Optional<String> responseMode;
    private Optional<Boolean> useNonce;
    private Optional<String> customParamResource;
    private String readTimeout;
    private Optional<Boolean> extractJwtAccessTokenClaims;

    public OidcConfigs(final com.typesafe.config.Config configs) {
        super(configs);
        clientId = getRequired(configs, OIDC_CLIENT_ID_CONFIG_PATH);
        clientSecret = getRequired(configs, OIDC_CLIENT_SECRET_CONFIG_PATH);
        discoveryUri = getRequired(configs, OIDC_DISCOVERY_URI_CONFIG_PATH);
        userNameClaim = getOptional(configs, OIDC_USERNAME_CLAIM_CONFIG_PATH, DEFAULT_OIDC_USERNAME_CLAIM);
        userNameClaimRegex =
            getOptional(configs, OIDC_USERNAME_CLAIM_REGEX_CONFIG_PATH, DEFAULT_OIDC_USERNAME_CLAIM_REGEX);
        scope = getOptional(configs, OIDC_SCOPE_CONFIG_PATH, DEFAULT_OIDC_SCOPE);
        clientName = getOptional(configs, OIDC_CLIENT_NAME_CONFIG_PATH, DEFAULT_OIDC_CLIENT_NAME);
        clientAuthenticationMethod = getOptional(configs, OIDC_CLIENT_AUTHENTICATION_METHOD_CONFIG_PATH,
            DEFAULT_OIDC_CLIENT_AUTHENTICATION_METHOD);
        jitProvisioningEnabled = Boolean.parseBoolean(
            getOptional(configs, OIDC_JIT_PROVISIONING_ENABLED_CONFIG_PATH, DEFAULT_OIDC_JIT_PROVISIONING_ENABLED));
        preProvisioningRequired = Boolean.parseBoolean(
            getOptional(configs, OIDC_PRE_PROVISIONING_REQUIRED_CONFIG_PATH, DEFAULT_OIDC_PRE_PROVISIONING_REQUIRED));
        extractGroupsEnabled = Boolean.parseBoolean(
            getOptional(configs, OIDC_EXTRACT_GROUPS_ENABLED, DEFAULT_OIDC_EXTRACT_GROUPS_ENABLED));
        groupsClaimName = getOptional(configs, OIDC_GROUPS_CLAIM_CONFIG_PATH_CONFIG_PATH, DEFAULT_OIDC_GROUPS_CLAIM);
        responseType = getOptional(configs, OIDC_RESPONSE_TYPE);
        responseMode = getOptional(configs, OIDC_RESPONSE_MODE);
        useNonce = getOptional(configs, OIDC_USE_NONCE).map(Boolean::parseBoolean);
        customParamResource = getOptional(configs, OIDC_CUSTOM_PARAM_RESOURCE);
        readTimeout = getOptional(configs, OIDC_READ_TIMEOUT, DEFAULT_OIDC_READ_TIMEOUT);
        extractJwtAccessTokenClaims = getOptional(configs, OIDC_EXTRACT_JWT_ACCESS_TOKEN_CLAIMS).map(Boolean::parseBoolean);
    }
}
