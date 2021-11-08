package auth;

import com.typesafe.config.Config;
import javax.inject.Inject;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static auth.AuthUtils.ACTOR;

/**
 * Implementation of base Play Authentication used to determine if a request to a route should be
 * authenticated.
 */
public class Authenticator extends Security.Authenticator {

    private boolean gmsAuthEnabled;

    @Inject
    public Authenticator(@Nonnull Config config) {
        this.gmsAuthEnabled = config.hasPath("gms.auth.enabled") && config.getBoolean("gms.auth.enabled");
    }

    @Override
    public String getUsername(@Nonnull Http.Context ctx) {
        // TODO: Consider whether frontend should validate the GMS token for an extra layer of security.
        if (this.gmsAuthEnabled) {
            // If gms auth is enabled, we only want to verify presence of the
            // "Authorization" header OR the presence of a frontend generated session cookie.
            // At this time, the actor is STILL CONSIDERED A GUEST
            return AuthUtils.isEligibleForForwarding(ctx) ? "urn:li:corpuser:UNKNOWN" : null;
        } else {
            // If gms auth is not enabled, we want to only verify the presence of the session cookie.
            return AuthUtils.hasValidSessionCookie(ctx) ? ctx.session().get(ACTOR) : null;
        }
    }

    @Override
    @Nonnull
    public Result onUnauthorized(@Nullable Http.Context ctx) {
        return unauthorized();
    }
}
