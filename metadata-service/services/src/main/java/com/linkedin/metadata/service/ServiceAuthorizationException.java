package com.linkedin.metadata.service;

/**
 * Thrown when a service-layer operation is rejected by the authorization layer. GraphQL document
 * resolvers translate this to {@code AuthorizationException}; this service-level type keeps the
 * module independent of the GraphQL module.
 */
public class ServiceAuthorizationException extends RuntimeException {
  public ServiceAuthorizationException(String message) {
    super(message);
  }
}
