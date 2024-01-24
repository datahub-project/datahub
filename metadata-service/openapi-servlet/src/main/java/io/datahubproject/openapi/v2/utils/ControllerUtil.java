package io.datahubproject.openapi.v2.utils;

import com.datahub.authentication.Actor;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.models.EntitySpec;
import io.datahubproject.openapi.exception.UnauthorizedException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ControllerUtil {
  private ControllerUtil() {}

  public static void checkAuthorized(
      @Nonnull Authorizer authorizationChain,
      @Nonnull Actor actor,
      @Nonnull EntitySpec entitySpec,
      @Nonnull List<String> privileges) {
    checkAuthorized(authorizationChain, actor, entitySpec, null, privileges);
  }

  public static void checkAuthorized(
      @Nonnull Authorizer authorizationChain,
      @Nonnull Actor actor,
      @Nonnull Set<EntitySpec> entitySpecs,
      @Nonnull List<String> privileges) {
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(ImmutableList.of(new ConjunctivePrivilegeGroup(privileges)));
    List<Optional<com.datahub.authorization.EntitySpec>> resourceSpecs =
        entitySpecs.stream()
            .map(
                entitySpec ->
                    Optional.of(new com.datahub.authorization.EntitySpec(entitySpec.getName(), "")))
            .collect(Collectors.toList());
    if (!AuthUtil.isAuthorizedForResources(
        authorizationChain, actor.toUrnStr(), resourceSpecs, orGroup)) {
      throw new UnauthorizedException(actor.toUrnStr() + " is unauthorized to get entities.");
    }
  }

  public static void checkAuthorized(
      @Nonnull Authorizer authorizationChain,
      @Nonnull Actor actor,
      @Nonnull EntitySpec entitySpec,
      @Nullable String entityUrn,
      @Nonnull List<String> privileges) {
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(ImmutableList.of(new ConjunctivePrivilegeGroup(privileges)));

    List<Optional<com.datahub.authorization.EntitySpec>> resourceSpecs =
        List.of(
            Optional.of(
                new com.datahub.authorization.EntitySpec(
                    entitySpec.getName(), entityUrn != null ? entityUrn : "")));
    if (!AuthUtil.isAuthorizedForResources(
        authorizationChain, actor.toUrnStr(), resourceSpecs, orGroup)) {
      throw new UnauthorizedException(actor.toUrnStr() + " is unauthorized to get entities.");
    }
  }
}
