package com.linkedin.datahub.graphql.resolvers;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.List;
import java.util.Optional;

public class AuthUtils {

  public static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  public static boolean isAuthorized(
      String principal, List<String> privilegeGroup, Authorizer authorizer) {
    for (final String privilege : privilegeGroup) {
      final AuthorizationRequest request =
          new AuthorizationRequest(principal, privilege, Optional.empty());
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        return false;
      }
    }
    return true;
  }

  private AuthUtils() {}
}
