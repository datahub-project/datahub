package com.datahub.metadata.authorization;

public interface Authorizer {
  AuthorizationResult authorize(AuthorizationRequest request);
}
