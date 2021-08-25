package com.linkedin.datahub.graphql.service;

import com.datahub.metadata.authorization.AuthorizationRequest;
import com.datahub.metadata.authorization.AuthorizationResult;
import com.datahub.metadata.authorization.Authorizer;


public class MockAuthorizationManager implements Authorizer {

  public MockAuthorizationManager() { }

  @Override
  public AuthorizationResult authorize(AuthorizationRequest request) {
    return new AuthorizationResult(null, null, AuthorizationResult.Type.ALLOW);
  }

}
