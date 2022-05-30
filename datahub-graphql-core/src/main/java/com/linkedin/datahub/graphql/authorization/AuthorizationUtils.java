package com.linkedin.datahub.graphql.authorization;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;
import com.datahub.authorization.ResourceSpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.Optional;
import javax.annotation.Nonnull;


public class AuthorizationUtils {

  public static boolean canManageUsersAndGroups(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE);
  }

  public static boolean canGeneratePersonalAccessToken(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE);
  }

  public static boolean canManageTokens(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_ACCESS_TOKENS);
  }

  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  public static boolean canManageGlossaries(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE);
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nonnull Optional<ResourceSpec> resourceSpec,
      @Nonnull PoliciesConfig.Privilege privilege) {
    final Authorizer authorizer = context.getAuthorizer();
    final String actor = context.getActorUrn();
    final ConjunctivePrivilegeGroup andGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()));
    return isAuthorized(authorizer, actor, resourceSpec, new DisjunctivePrivilegeGroup(ImmutableList.of(andGroup)));
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    return isAuthorized(authorizer, actor, Optional.empty(), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    final ResourceSpec resourceSpec = new ResourceSpec(resourceType, resource);
    return isAuthorized(authorizer, actor, Optional.of(resourceSpec), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull Optional<ResourceSpec> maybeResourceSpec,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    for (ConjunctivePrivilegeGroup andPrivilegeGroup : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      // If any conjunctive privilege group is authorized, then the entire request is authorized.
      if (isAuthorized(authorizer, actor, andPrivilegeGroup, maybeResourceSpec)) {
        return true;
      }
    }
    // If none of the disjunctive privilege groups were authorized, then the entire request is not authorized.
    return false;
  }

  private static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nonnull Optional<ResourceSpec> resourceSpec) {
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

  private AuthorizationUtils() { }

}

