package com.linkedin.datahub.graphql.resolvers;

import java.util.List;
import java.util.Optional;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;

public class AuthUtils {

  public static boolean isAuthorized(
      String principal,
      List<String> privilegeGroup,
      Authorizer authorizer) {
    for (final String privilege : privilegeGroup) {
      final AuthorizationRequest request = new AuthorizationRequest(principal, privilege, Optional.empty());
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        return false;
      }
    }
    return true;
  }


  private AuthUtils() { }
}
