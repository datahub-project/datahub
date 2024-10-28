package auth.sso.oidc;

import auth.sso.SsoProvider;
import auth.sso.oidc.custom.CustomOidcClient;
import com.nimbusds.jose.JWSAlgorithm;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.profile.OidcProfileDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger logger = LoggerFactory.getLogger(OidcProvider.class);
  private static final String OIDC_CLIENT_NAME = "oidc";

  private final OidcConfigs oidcConfigs;
  private final Client oidcClient; // Used primarily for redirecting to IdP.

  public OidcProvider(final OidcConfigs configs) {
    oidcConfigs = configs;
    oidcClient = createPac4jClient();
  }

  @Override
  public Client client() {
    return oidcClient;
  }

  @Override
  public OidcConfigs configs() {
    return oidcConfigs;
  }

  @Override
  public SsoProtocol protocol() {
    return SsoProtocol.OIDC;
  }

  private Client createPac4jClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(oidcConfigs.getClientId());
    oidcConfiguration.setSecret(oidcConfigs.getClientSecret());
    oidcConfiguration.setDiscoveryURI(oidcConfigs.getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(
        oidcConfigs.getClientAuthenticationMethod());
    oidcConfiguration.setScope(oidcConfigs.getScope());
    try {
      oidcConfiguration.setConnectTimeout(Integer.parseInt(oidcConfigs.getConnectTimeout()));
    } catch (NumberFormatException e) {
      log.warn("Invalid connect timeout configuration, defaulting to 1000ms");
    }
    try {
      oidcConfiguration.setReadTimeout(Integer.parseInt(oidcConfigs.getReadTimeout()));
    } catch (NumberFormatException e) {
      log.warn("Invalid read timeout configuration, defaulting to 5000ms");
    }
    oidcConfigs.getResponseType().ifPresent(oidcConfiguration::setResponseType);
    oidcConfigs.getResponseMode().ifPresent(oidcConfiguration::setResponseMode);
    oidcConfigs.getUseNonce().ifPresent(oidcConfiguration::setUseNonce);
    Map<String, String> customParamsMap = new HashMap<>();
    oidcConfigs.getCustomParamResource().ifPresent(value -> customParamsMap.put("resource", value));
    oidcConfigs.getGrantType().ifPresent(value -> customParamsMap.put("grant_type", value));
    oidcConfigs.getAcrValues().ifPresent(value -> customParamsMap.put("acr_values", value));
    if (!customParamsMap.isEmpty()) {
      oidcConfiguration.setCustomParams(customParamsMap);
    }
    oidcConfigs
        .getPreferredJwsAlgorithm()
        .ifPresent(
            preferred -> {
              log.info("Setting preferredJwsAlgorithm: " + preferred);
              oidcConfiguration.setPreferredJwsAlgorithm(JWSAlgorithm.parse(preferred));
            });

    // Enable state parameter validation
    oidcConfiguration.setWithState(true);

    final CustomOidcClient oidcClient = new CustomOidcClient(oidcConfiguration);
    oidcClient.setName(OIDC_CLIENT_NAME);
    oidcClient.setCallbackUrl(oidcConfigs.getAuthBaseUrl() + oidcConfigs.getAuthBaseCallbackPath());
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    oidcClient.addAuthorizationGenerator(
        new OidcAuthorizationGenerator(new OidcProfileDefinition(), oidcConfigs));

    return oidcClient;
  }
}
