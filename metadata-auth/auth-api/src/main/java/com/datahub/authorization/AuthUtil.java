package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

public class AuthUtil {

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull Optional<EntitySpec> maybeResourceSpec,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    for (ConjunctivePrivilegeGroup andPrivilegeGroup :
        privilegeGroup.getAuthorizedPrivilegeGroups()) {
      // If any conjunctive privilege group is authorized, then the entire request is authorized.
      if (isAuthorized(authorizer, actor, andPrivilegeGroup, maybeResourceSpec)) {
        return true;
      }
    }
    // If none of the disjunctive privilege groups were authorized, then the entire request is not
    // authorized.
    return false;
  }

  public static boolean isAuthorizedForResources(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull List<Optional<EntitySpec>> resourceSpecs,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    for (ConjunctivePrivilegeGroup andPrivilegeGroup :
        privilegeGroup.getAuthorizedPrivilegeGroups()) {
      // If any conjunctive privilege group is authorized, then the entire request is authorized.
      if (isAuthorizedForResources(authorizer, actor, andPrivilegeGroup, resourceSpecs)) {
        return true;
      }
    }
    // If none of the disjunctive privilege groups were authorized, then the entire request is not
    // authorized.
    return false;
  }

  private static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nonnull Optional<EntitySpec> resourceSpec) {
    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      final AuthorizationRequest request = new AuthorizationRequest(actor, privilege, resourceSpec);
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  private static boolean isAuthorizedForResources(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nonnull List<Optional<EntitySpec>> resourceSpecs) {
    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      for (Optional<EntitySpec> resourceSpec : resourceSpecs) {
        final AuthorizationRequest request =
            new AuthorizationRequest(actor, privilege, resourceSpec);
        final AuthorizationResult result = authorizer.authorize(request);
        if (AuthorizationResult.Type.DENY.equals(result.getType())) {
          // Short circuit.
          return false;
        }
      }
    }
    return true;
  }

  private AuthUtil() {}
}
