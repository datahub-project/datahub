package auth;

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
    @Override
    public String getUsername(@Nonnull Http.Context ctx) {
        return AuthUtils.isAuthenticated(ctx) ? ctx.session().get(ACTOR) : null;
    }

    @Override
    @Nonnull
    public Result onUnauthorized(@Nullable Http.Context ctx) {
        return unauthorized();
    }
}
