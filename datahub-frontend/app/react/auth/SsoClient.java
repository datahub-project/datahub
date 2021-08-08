package react.auth;

import org.pac4j.core.client.Client;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;
import play.mvc.Result;


/**
 * Thin warpper around a Pac4j {@link Client}.
 */
public interface SsoClient {

  Client getClient();

  Result handleCallback(final Result result, final PlayWebContext context, ProfileManager<?> profileManager);

}
