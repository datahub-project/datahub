package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TestAuthSession implements AuthorizationSession {
  public static AuthorizationSession ALLOW_ALL =
      from((priv, authorizer) -> new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

  public static AuthorizationSession from(Authentication auth, Authorizer authorizer) {
    return from(
        (privilege, resourceSpec) -> {
          final AuthorizationRequest request =
              new AuthorizationRequest(
                  auth.getActor().toUrnStr(), privilege, Optional.ofNullable(resourceSpec));
          return authorizer.authorize(request);
        });
  }

  public static AuthorizationSession from(
      BiFunction<String, EntitySpec, AuthorizationResult> authFunction) {
    return new TestAuthSession(authFunction);
  }

  private final BiFunction<String, EntitySpec, AuthorizationResult> authFunction;

  public TestAuthSession(BiFunction<String, EntitySpec, AuthorizationResult> authFunction) {
    this.authFunction = authFunction;
  }

  @Override
  public AuthorizationResult authorize(
      @Nonnull String privilege, @Nullable EntitySpec resourceSpec) {
    return authFunction.apply(privilege, resourceSpec);
  }
}
