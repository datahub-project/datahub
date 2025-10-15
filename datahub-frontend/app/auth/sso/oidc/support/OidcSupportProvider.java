package auth.sso.oidc.support;

import auth.sso.oidc.OidcProvider;
import auth.sso.oidc.custom.CustomOidcClient;
import com.nimbusds.jose.JWSAlgorithm;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.profile.OidcProfileDefinition;

/** A Pac4j OIDC client wrapper for support staff authentication that extends OidcProvider. */
@Slf4j
public class OidcSupportProvider extends OidcProvider {
  private static final String OIDC_SUPPORT_CLIENT_NAME = "oidc";

  public OidcSupportProvider(final OidcSupportConfigs configs) {
    super(configs);
    log.info("Initialized OIDC Support Provider with client name: {}", configs.getClientName());
  }

  @Override
  public OidcSupportConfigs configs() {
    return (OidcSupportConfigs) super.configs();
  }

  @Override
  protected Client createPac4jClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(configs().getClientId());
    oidcConfiguration.setSecret(configs().getClientSecret());
    oidcConfiguration.setDiscoveryURI(configs().getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(
        configs().getClientAuthenticationMethod());
    oidcConfiguration.setScope(configs().getScope());

    // Set timeouts
    try {
      oidcConfiguration.setConnectTimeout(Integer.parseInt(configs().getConnectTimeout()));
    } catch (NumberFormatException e) {
      log.warn("Invalid connect timeout configuration, defaulting to 1000ms");
    }
    try {
      oidcConfiguration.setReadTimeout(Integer.parseInt(configs().getReadTimeout()));
    } catch (NumberFormatException e) {
      log.warn("Invalid read timeout configuration, defaulting to 5000ms");
    }

    // Set optional parameters
    configs().getResponseType().ifPresent(oidcConfiguration::setResponseType);
    configs().getResponseMode().ifPresent(oidcConfiguration::setResponseMode);
    configs().getUseNonce().ifPresent(oidcConfiguration::setUseNonce);

    // Set custom parameters
    Map<String, String> customParamsMap = new HashMap<>();
    configs().getCustomParamResource().ifPresent(value -> customParamsMap.put("resource", value));
    configs().getGrantType().ifPresent(value -> customParamsMap.put("grant_type", value));
    configs().getAcrValues().ifPresent(value -> customParamsMap.put("acr_values", value));
    if (!customParamsMap.isEmpty()) {
      oidcConfiguration.setCustomParams(customParamsMap);
    }

    // Set preferred JWS algorithm
    configs()
        .getPreferredJwsAlgorithm()
        .ifPresent(
            preferred -> {
              log.info("Setting preferredJwsAlgorithm: " + preferred);
              oidcConfiguration.setPreferredJwsAlgorithm(JWSAlgorithm.parse(preferred));
            });

    // Enable state parameter validation
    oidcConfiguration.setWithState(true);

    // Use the configs directly since OidcSupportConfigs extends OidcConfigs
    final CustomOidcClient oidcClient = new CustomOidcClient(oidcConfiguration, configs());
    oidcClient.setName(OIDC_SUPPORT_CLIENT_NAME);
    oidcClient.setCallbackUrl(configs().getAuthBaseUrl() + "/support/callback");
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    oidcClient.addAuthorizationGenerator(
        new auth.sso.oidc.OidcAuthorizationGenerator(new OidcProfileDefinition(), configs()));

    return oidcClient;
  }
}
