package io.datahubproject.test.metadata.context;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.LazyAuthorizationResultMap;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class TestAuthSession implements AuthorizationSession {
  public static AuthorizationSession DENY_ALL =
      from(
          (priv, entitySpec) ->
              new AuthorizationResult(
                  new AuthorizationRequest(null, priv, Optional.ofNullable(entitySpec), List.of()),
                  AuthorizationResult.Type.DENY,
                  ""));

  public static AuthorizationSession allowOnly(EntitySpec entitySpec, Set<String> privileges) {
    return from(
        (privilege, resourceSpec) -> {
          AuthorizationRequest request =
              new AuthorizationRequest(
                  null, privilege, Optional.ofNullable(resourceSpec), List.of());
          if (Objects.equals(entitySpec, resourceSpec) && privileges.contains(privilege)) {
            return new AuthorizationResult(
                request, AuthorizationResult.Type.ALLOW, "Allowed via TestAuthSession");
          }
          return new AuthorizationResult(
              request,
              AuthorizationResult.Type.DENY,
              "Either entity spec does not match or privilege is not allowed");
        });
  }

  public static AuthorizationSession allowAnyFor(EntitySpec entitySpec) {
    return from(
        (privilege, resourceSpec) -> {
          AuthorizationRequest request =
              new AuthorizationRequest(
                  null, privilege, Optional.ofNullable(resourceSpec), List.of());
          if (Objects.equals(entitySpec, resourceSpec)) {
            return new AuthorizationResult(
                request, AuthorizationResult.Type.ALLOW, "Allowed via TestAuthSession");
          }
          return new AuthorizationResult(
              request, AuthorizationResult.Type.DENY, "Entity spec does not match");
        });
  }

  public static AuthorizationSession from(Authentication auth, Authorizer authorizer) {
    return from(
        (privilege, resourceSpec) -> {
          final AuthorizationRequest request =
              new AuthorizationRequest(
                  auth.getActor().toUrnStr(),
                  privilege,
                  Optional.ofNullable(resourceSpec),
                  Collections.emptyList());
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
  public BatchAuthorizationResult authorize(
      @Nonnull final Set<String> privileges,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull Collection<EntitySpec> subResources) {
    return new BatchAuthorizationResult(
        null,
        new LazyAuthorizationResultMap(
            privileges, privilege -> authFunction.apply(privilege, resourceSpec)));
  }
}
