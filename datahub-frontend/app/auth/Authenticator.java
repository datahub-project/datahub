package auth;

import static auth.AuthUtils.*;

import com.typesafe.config.Config;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;

/**
 * Implementation of base Play Authentication used to determine if a request to a route should be
 * authenticated.
 */
public class Authenticator extends Security.Authenticator {

  private final boolean metadataServiceAuthEnabled;

  @Inject
  public Authenticator(@Nonnull Config config) {
    this.metadataServiceAuthEnabled =
        config.hasPath(METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH)
            && config.getBoolean(METADATA_SERVICE_AUTH_ENABLED_CONFIG_PATH);
  }

  @Override
  public Optional<String> getUsername(@Nonnull Http.Request req) {
    if (this.metadataServiceAuthEnabled) {
      // If Metadata Service auth is enabled, we only want to verify presence of the
      // "Authorization" header OR the presence of a frontend generated session cookie.
      // At this time, the actor is still considered to be unauthenicated.
      return Optional.ofNullable(
          AuthUtils.isEligibleForForwarding(req) ? "urn:li:corpuser:UNKNOWN" : null);
    } else {
      // If Metadata Service auth is not enabled, verify the presence of a valid session cookie.
      return Optional.ofNullable(
          AuthUtils.hasValidSessionCookie(req) ? req.session().data().get(ACTOR) : null);
    }
  }

  @Override
  @Nonnull
  public Result onUnauthorized(@Nullable Http.Request req) {
    return unauthorized();
  }
}
