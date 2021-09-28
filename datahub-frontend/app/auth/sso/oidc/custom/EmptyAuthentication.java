package auth.sso.oidc.custom;

import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;


public class EmptyAuthentication extends ClientAuthentication {
  /**
   * Creates a new "None" client authentication.
   *
   * @param clientID The client identifier. Must not be {@code null}.
   */
  protected EmptyAuthentication(ClientID clientID) {
    super(ClientAuthenticationMethod.NONE, clientID);
  }

  @Override
  public void applyTo(HTTPRequest httpRequest) {
    return;
  }
}
