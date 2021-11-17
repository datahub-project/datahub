package com.datahub.authentication;

/**
 * A set of shared constants related to Authentication.
 */
public class AuthenticationConstants {

  public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
  public static final String DELEGATED_FOR_ACTOR_HEADER_NAME = "X-DataHub-Actor";
  public static final String SYSTEM_CLIENT_ID_CONFIG = "systemClientId";
  public static final String SYSTEM_CLIENT_SECRET_CONFIG = "systemClientSecret";

  private AuthenticationConstants() { }
}
