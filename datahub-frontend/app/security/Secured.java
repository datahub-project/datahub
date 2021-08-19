package security;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import play.mvc.Http.Context;
import play.mvc.Result;
import play.mvc.Security;


public class Secured extends Security.Authenticator {
  @Override
  @Nonnull
  public String getUsername(@Nonnull Context ctx) {
    return ctx.session().get("user");
  }

  @Override
  @Nonnull
  public Result onUnauthorized(@Nullable Context ctx) {
    return unauthorized();
  }
}
