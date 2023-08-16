package com.linkedin.datahub.graphql.exception;


/**
 * Exception thrown when update fails due to ASSOCIATE_TAG privilege.
 */
public class TagAuthorizationException extends DataHubGraphQLException {

  public TagAuthorizationException(String message) {
    super(message, DataHubGraphQLErrorCode.UNAUTHORIZED_TAG_ERROR);
  }

  public TagAuthorizationException(String message, Throwable cause) {
    super(message, DataHubGraphQLErrorCode.UNAUTHORIZED_TAG_ERROR);
  }
}
