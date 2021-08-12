package react.auth;

import org.pac4j.core.client.Client;


public interface SsoProvider<C extends SsoConfigs> {

  /**
   * The protocol used for SSO.
   */
  enum SsoProtocol {
    OIDC("oidc");
    // SAML -- not yet supported.

    // Common name appears in the Callback URL itself.
    private final String _commonName;

    public String getCommonName() {
      return _commonName;
    }

    SsoProtocol(String commonName) {
      _commonName = commonName;
    }
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
