package react.auth;

import org.pac4j.core.client.Client;


public interface SsoProvider<C extends SsoConfigs> {

  /**
   * The protocol used for SSO.
   */
  enum SsoProtocol {
    OIDC,
    // SAML -- not yet supported.
  }

  C configs();

  /**
   * Retrieves an initialized Pac4j client.
   */
  SsoProtocol protocol();

  /**
   * Retrieves an initialized Pac4j client.
   */
  Client<?, ?> client();

}
