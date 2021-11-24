package com.linkedin.datahub.graphql.service;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;


/**
 * Implementation of {@link Authorizer} that allows all authorization requests. Used to keep backwards
 * compatibility with the deprecated DataHub Standalone GraphQL service.
 */
public class AllowAllAuthorizer implements Authorizer {

  public AllowAllAuthorizer() { }

  @Override
  public AuthorizationResult authorize(AuthorizationRequest request) {
    return new AuthorizationResult(null, null, AuthorizationResult.Type.ALLOW);
  }

  @Override
  public AuthorizationMode mode() {
    return AuthorizationMode.ALLOW_ALL;
  }
}
