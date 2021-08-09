package react.auth;

import org.pac4j.core.client.Client;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;


/**
 * Wrapper around Pac4j abstractions of a {@link Client} and {@link org.pac4j.core.engine.CallbackLogic} responsible
 * for providing a client used during authentication along with logic related to handling
 * the response provided by an Identity provider on redirect to DataHub.
 */
public interface SsoProvider {

  /**
   * Returns the Pac4j {@link Client} object associated with the flavor of Sso (e.g. OIDC or SAML)
   */
  Client<?, ?> getClient();

  /**
   * Returns the Pac4j {@link CallbackLogic} to be executed on Identity Provider redirect back to the
   * service provider.
   */
  CallbackLogic<Result, PlayWebContext> getCallbackLogic();

}
