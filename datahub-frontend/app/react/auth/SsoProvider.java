package react.auth;

import org.pac4j.core.client.Client;

/**
 * Wrapper around Pac4j abstractions of a {@link Client}.
 */
public interface SsoProvider<C extends SsoConfigs> {

  /**
   * Returns the configurations associated with the current SsoProvider.
   */
  C getConfigs();

  /**
   * Returns the Pac4j {@link Client} object associated with the flavor of Sso (e.g. OIDC or SAML)
   */
  Client<?, ?> getClient();

}
