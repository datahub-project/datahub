package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;

/** Used for SystemUpdate processes which require passing authorization checks * */
public class SystemUpdateAuthorizer implements Authorizer {
  @Override
  public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
    if (Constants.SYSTEM_ACTOR.equals(request.getActorUrn())) {
      return new AuthorizationResult(
          request,
          AuthorizationResult.Type.ALLOW,
          String.format("Granted by system actor " + request.getActorUrn()));
    }

    return new AuthorizationResult(
        request, AuthorizationResult.Type.DENY, "Only system user is allowed.");
  }
}
