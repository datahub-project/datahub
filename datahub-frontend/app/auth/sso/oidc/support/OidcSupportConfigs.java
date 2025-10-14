package auth.sso.oidc.support;

import static auth.AuthUtils.*;
import static auth.ConfigUtil.*;

import auth.sso.oidc.OidcConfigs;
import com.typesafe.config.Config;
import java.util.Objects;
import lombok.Getter;

/** Class responsible for extracting and validating Support OIDC related configurations. */
@Getter
public class OidcSupportConfigs extends OidcConfigs {

  /** Support-specific additional fields */
  private final String groupClaim;

  private final String roleClaim;

  private final String userPictureLink;

  private final String defaultRole;

  /** Support-specific default values */
  public static final String DEFAULT_SUPPORT_GROUP_NAME = "__datahub_support";

  private static final String DEFAULT_OIDC_SUPPORT_GROUP_CLAIM = "";

  private static final String DEFAULT_OIDC_SUPPORT_ROLE_CLAIM = "role";

  private static final String DEFAULT_OIDC_SUPPORT_USER_PICTURE_LINK = "";

  private static final String DEFAULT_OIDC_SUPPORT_DEFAULT_ROLE = "Admin";

  public static final String DEFAULT_SUPPORT_USER_FALLBACK_PICTURE =
      "assets/logos/acryl-dark-mark.svg";

  public OidcSupportConfigs(Builder builder) {
    super(builder);
    this.groupClaim = builder.groupClaim;
    this.roleClaim = builder.roleClaim;
    this.userPictureLink = builder.userPictureLink;
    this.defaultRole = builder.defaultRole;
  }

  public static class Builder extends OidcConfigs.Builder {
    private String groupClaim = DEFAULT_OIDC_SUPPORT_GROUP_CLAIM;
    private String roleClaim = DEFAULT_OIDC_SUPPORT_ROLE_CLAIM;
    private String userPictureLink = DEFAULT_OIDC_SUPPORT_USER_PICTURE_LINK;
    private String defaultRole = DEFAULT_OIDC_SUPPORT_DEFAULT_ROLE;

    public Builder from(final com.typesafe.config.Config configs) {
      // Parse support-specific configs first
      groupClaim =
          getOptional(configs, "auth.oidc.support.groupClaim", DEFAULT_OIDC_SUPPORT_GROUP_CLAIM);
      roleClaim =
          getOptional(configs, "auth.oidc.support.roleClaim", DEFAULT_OIDC_SUPPORT_ROLE_CLAIM);
      userPictureLink =
          getOptional(
              configs, "auth.oidc.support.userPictureLink", DEFAULT_OIDC_SUPPORT_USER_PICTURE_LINK);
      defaultRole =
          getOptional(configs, "auth.oidc.support.defaultRole", DEFAULT_OIDC_SUPPORT_DEFAULT_ROLE);

      // Parse regular OIDC configs using support namespace
      super.from(createSupportConfigMap(configs));
      return this;
    }

    public Builder from(final com.typesafe.config.Config configs, final String ssoSettingsJsonStr) {
      // Initialize jsonNode first by calling parent's JSON parsing
      super.from(ssoSettingsJsonStr);

      // Parse support-specific JSON configs
      if (jsonNode.has("groupClaim")) {
        groupClaim = jsonNode.get("groupClaim").asText();
      }
      if (jsonNode.has("roleClaim")) {
        roleClaim = jsonNode.get("roleClaim").asText();
      }
      if (jsonNode.has("userPictureLink")) {
        userPictureLink = jsonNode.get("userPictureLink").asText();
      }
      if (jsonNode.has("defaultRole")) {
        defaultRole = jsonNode.get("defaultRole").asText();
      }

      // Parse regular OIDC configs using support namespace
      super.from(createSupportConfigMap(configs));
      return this;
    }

    /**
     * Creates a config map that maps support OIDC configs to regular OIDC config paths so we can
     * reuse the parent OidcConfigs parsing logic.
     */
    private Config createSupportConfigMap(Config configs) {
      java.util.Map<String, String> configMap = new java.util.HashMap<>();

      // Map support configs to regular OIDC config paths
      if (configs.hasPath("auth.oidc.support.clientId")) {
        configMap.put("auth.oidc.clientId", configs.getString("auth.oidc.support.clientId"));
      }
      if (configs.hasPath("auth.oidc.support.clientSecret")) {
        configMap.put(
            "auth.oidc.clientSecret", configs.getString("auth.oidc.support.clientSecret"));
      }
      if (configs.hasPath("auth.oidc.support.discoveryUri")) {
        configMap.put(
            "auth.oidc.discoveryUri", configs.getString("auth.oidc.support.discoveryUri"));
      }
      if (configs.hasPath("auth.oidc.support.userNameClaim")) {
        configMap.put(
            "auth.oidc.userNameClaim", configs.getString("auth.oidc.support.userNameClaim"));
      }
      if (configs.hasPath("auth.oidc.support.userNameClaimRegex")) {
        configMap.put(
            "auth.oidc.userNameClaimRegex",
            configs.getString("auth.oidc.support.userNameClaimRegex"));
      }
      if (configs.hasPath("auth.oidc.support.scope")) {
        configMap.put("auth.oidc.scope", configs.getString("auth.oidc.support.scope"));
      }
      if (configs.hasPath("auth.oidc.support.clientName")) {
        configMap.put("auth.oidc.clientName", configs.getString("auth.oidc.support.clientName"));
      }
      if (configs.hasPath("auth.oidc.support.clientAuthenticationMethod")) {
        configMap.put(
            "auth.oidc.clientAuthenticationMethod",
            configs.getString("auth.oidc.support.clientAuthenticationMethod"));
      }
      if (configs.hasPath("auth.oidc.support.responseType")) {
        configMap.put(
            "auth.oidc.responseType", configs.getString("auth.oidc.support.responseType"));
      }
      if (configs.hasPath("auth.oidc.support.responseMode")) {
        configMap.put(
            "auth.oidc.responseMode", configs.getString("auth.oidc.support.responseMode"));
      }
      if (configs.hasPath("auth.oidc.support.useNonce")) {
        configMap.put("auth.oidc.useNonce", configs.getString("auth.oidc.support.useNonce"));
      }
      if (configs.hasPath("auth.oidc.support.customParam.resource")) {
        configMap.put(
            "auth.oidc.customParam.resource",
            configs.getString("auth.oidc.support.customParam.resource"));
      }
      if (configs.hasPath("auth.oidc.support.readTimeout")) {
        configMap.put("auth.oidc.readTimeout", configs.getString("auth.oidc.support.readTimeout"));
      }
      if (configs.hasPath("auth.oidc.support.connectTimeout")) {
        configMap.put(
            "auth.oidc.connectTimeout", configs.getString("auth.oidc.support.connectTimeout"));
      }
      if (configs.hasPath("auth.oidc.support.extractJwtAccessTokenClaims")) {
        configMap.put(
            "auth.oidc.extractJwtAccessTokenClaims",
            configs.getString("auth.oidc.support.extractJwtAccessTokenClaims"));
      }
      if (configs.hasPath("auth.oidc.support.preferredJwsAlgorithm")) {
        configMap.put(
            "auth.oidc.preferredJwsAlgorithm",
            configs.getString("auth.oidc.support.preferredJwsAlgorithm"));
      }
      if (configs.hasPath("auth.oidc.support.grantType")) {
        configMap.put("auth.oidc.grantType", configs.getString("auth.oidc.support.grantType"));
      }
      if (configs.hasPath("auth.oidc.support.acrValues")) {
        configMap.put("auth.oidc.acrValues", configs.getString("auth.oidc.support.acrValues"));
      }
      if (configs.hasPath("auth.oidc.support.httpRetryAttempts")) {
        configMap.put(
            "auth.oidc.httpRetryAttempts",
            configs.getString("auth.oidc.support.httpRetryAttempts"));
      }
      if (configs.hasPath("auth.oidc.support.httpRetryDelay")) {
        configMap.put(
            "auth.oidc.httpRetryDelay", configs.getString("auth.oidc.support.httpRetryDelay"));
      }

      // Always set support-specific defaults
      configMap.put("auth.oidc.jitProvisioningEnabled", "true");
      configMap.put("auth.oidc.preProvisioningRequired", "false");
      configMap.put("auth.oidc.extractGroupsEnabled", "false");
      configMap.put("auth.oidc.groupsClaim", "groups");

      // Copy base URL
      if (configs.hasPath("auth.baseUrl")) {
        configMap.put("auth.baseUrl", configs.getString("auth.baseUrl"));
      }

      return com.typesafe.config.ConfigFactory.parseMap(configMap);
    }

    public OidcSupportConfigs build() {
      Objects.requireNonNull(oidcEnabled, "oidcEnabled is required");
      Objects.requireNonNull(clientId, "clientId is required");
      Objects.requireNonNull(clientSecret, "clientSecret is required");
      Objects.requireNonNull(discoveryUri, "discoveryUri is required");
      Objects.requireNonNull(authBaseUrl, "authBaseUrl is required");

      return new OidcSupportConfigs(this);
    }
  }
}
