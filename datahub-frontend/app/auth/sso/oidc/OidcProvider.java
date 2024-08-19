package auth.sso.oidc;

import auth.sso.SsoProvider;
import auth.sso.oidc.custom.CustomOidcClient;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfileDefinition;

/**
 * Implementation of {@link SsoProvider} supporting the OIDC protocol.
 *
 * <p>This class is a thin wrapper over a Pac4J {@link Client} object and all DataHub-specific OIDC
 * related configuration options, which reside in an instance of {@link OidcConfigs}.
 *
 * <p>It is responsible for initializing this client from a configuration object ({@link
 * OidcConfigs}. Note that this class is not related to the logic performed when an IdP performs a
 * callback to DataHub.
 */
@Slf4j
public class OidcProvider implements SsoProvider<OidcConfigs> {

  private static final String OIDC_CLIENT_NAME = "oidc";

  private final OidcConfigs _oidcConfigs;
  private final Client<OidcCredentials> _oidcClient; // Used primarily for redirecting to IdP.

  public OidcProvider(final OidcConfigs configs) {
    _oidcConfigs = configs;
    _oidcClient = createPac4jClient();
  }

  @Override
  public Client<OidcCredentials> client() {
    return _oidcClient;
  }

  @Override
  public OidcConfigs configs() {
    return _oidcConfigs;
  }

  @Override
  public SsoProtocol protocol() {
    return SsoProtocol.OIDC;
  }

  private Client<OidcCredentials> createPac4jClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(_oidcConfigs.getClientId());
    oidcConfiguration.setSecret(_oidcConfigs.getClientSecret());
    oidcConfiguration.setDiscoveryURI(_oidcConfigs.getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(
        _oidcConfigs.getClientAuthenticationMethod());
    oidcConfiguration.setScope(_oidcConfigs.getScope());
    try {
      oidcConfiguration.setReadTimeout(Integer.parseInt(_oidcConfigs.getReadTimeout()));
    } catch (NumberFormatException e) {
      log.warn("Invalid read timeout configuration, defaulting to 5000ms");
    }
    _oidcConfigs.getResponseType().ifPresent(oidcConfiguration::setResponseType);
    _oidcConfigs.getResponseMode().ifPresent(oidcConfiguration::setResponseMode);
    _oidcConfigs.getUseNonce().ifPresent(oidcConfiguration::setUseNonce);
    Map<String, String> customParamsMap = new HashMap<>();
    _oidcConfigs
        .getCustomParamResource()
        .ifPresent(value -> customParamsMap.put("resource", value));
    _oidcConfigs
        .getGrantType()
        .ifPresent(value -> customParamsMap.put("grant_type", value));
    _oidcConfigs
        .getAcrValues()
        .ifPresent(value -> customParamsMap.put("acr_values", value));
    if (!customParamsMap.isEmpty()) {
      oidcConfiguration.setCustomParams(customParamsMap);
    }
    _oidcConfigs
        .getPreferredJwsAlgorithm()
        .ifPresent(
            preferred -> {
              log.info("Setting preferredJwsAlgorithm: " + preferred);
              oidcConfiguration.setPreferredJwsAlgorithm(preferred);
            });

    final CustomOidcClient oidcClient = new CustomOidcClient(oidcConfiguration);
    oidcClient.setName(OIDC_CLIENT_NAME);
    oidcClient.setCallbackUrl(
        _oidcConfigs.getAuthBaseUrl() + _oidcConfigs.getAuthBaseCallbackPath());
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    oidcClient.addAuthorizationGenerator(
        new OidcAuthorizationGenerator(new OidcProfileDefinition(), _oidcConfigs));
    return oidcClient;
  }
}
