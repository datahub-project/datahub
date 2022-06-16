package com.datahub.authentication;

/**
 * A set of shared constants related to Authentication.
 */
public class AuthenticationConstants {

  /**
   * Name of the header which carries authorization information
   */
  public static final String AUTHORIZATION_HEADER_NAME = "Authorization";

  /**
   * A deprecated header that previously carried the urn of the authenticated actor.
   * This has been replaced by the DELEGATED_FOR_ACTOR_ID and DELEGATED_FOR_ACTOR_TYPE headers.
   */
  public static final String LEGACY_X_DATAHUB_ACTOR_HEADER = "X-DataHub-Actor";

  /**
   * A header capturing the unique Actor Id that is delegating a request.
   */
  public static final String DELEGATED_FOR_ACTOR_ID_HEADER_NAME = "X-DataHub-Delegated-For-Id";

  /**
   * A header capturing the unique Actor Type that is delegating a request.
   */
  public static final String DELEGATED_FOR_ACTOR_TYPE_HEADER_NAME = "X-DataHub-Delegated-For-Type";

  public static final String SYSTEM_CLIENT_ID_CONFIG = "systemClientId";
  public static final String SYSTEM_CLIENT_SECRET_CONFIG = "systemClientSecret";

  public static final String ENTITY_SERVICE = "entityService";
  public static final String TOKEN_SERVICE = "tokenService";


  private AuthenticationConstants() { }
}
