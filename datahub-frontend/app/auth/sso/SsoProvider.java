package auth.sso;

import org.pac4j.core.client.Client;
import org.pac4j.core.credentials.Credentials;

/** A thin interface over a Pac4j {@link Client} object and its associated configurations. */
public interface SsoProvider<C extends SsoConfigs> {

  /** The protocol used for SSO. */
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

  /** Returns the configs required by the provider. */
  C configs();

  /** Returns the SSO protocol associated with the provider instance. */
  SsoProtocol protocol();

  /** Retrieves an initialized Pac4j {@link Client}. */
  Client<? extends Credentials> client();
}
