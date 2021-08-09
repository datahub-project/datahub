package react.auth;

import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;


public class OidcProvider implements SsoProvider<OidcConfigs> {

  private final OidcConfigs _oidcConfigs;
  private final Client<OidcCredentials, OidcProfile> _oidcClient;

  public OidcProvider(OidcConfigs configs) {
    _oidcConfigs = configs;
    _oidcClient = createOidcClient();
  }

  @Override
  public OidcConfigs getConfigs() {
    return _oidcConfigs;
  }

  @Override
  public Client<OidcCredentials, OidcProfile> getClient() {
    return _oidcClient;
  }

  private Client<OidcCredentials, OidcProfile> createOidcClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(_oidcConfigs.getClientId());
    oidcConfiguration.setSecret(_oidcConfigs.getClientSecret());
    oidcConfiguration.setDiscoveryURI(_oidcConfigs.getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(_oidcConfigs.getClientAuthenticationMethod());
    oidcConfiguration.setScope(_oidcConfigs.getScope());

    final org.pac4j.oidc.client.OidcClient<OidcProfile, OidcConfiguration>  oidcClient = new org.pac4j.oidc.client.OidcClient<>(oidcConfiguration);
    oidcClient.setName(_oidcConfigs.getClientName());
    oidcClient.setCallbackUrl(_oidcConfigs.getAuthBaseUrl() + _oidcConfigs.getAuthBaseCallbackPath());
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    return oidcClient;
  }
}
