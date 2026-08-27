package com.linkedin.datahub.graphql.exception;

import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.entity.validation.ValidationException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps metadata ingest/validation failures to GraphQL-layer exceptions with correct HTTP codes. */
public final class IngestProposalExceptionUtils {

  private IngestProposalExceptionUtils() {}

  /**
   * Converts an ingest failure into a GraphQL exception: authorization failures become {@link
   * AuthorizationException} (403), other validation failures become {@link ValidationException}
   * (400). Unexpected errors remain {@link RuntimeException} (500).
   */
  @Nonnull
  public static RuntimeException toGraphQLException(
      @Nonnull String operationDescription, @Nonnull Exception exception) {
    ValidationException validationException = findCause(exception, ValidationException.class);
    if (validationException != null) {
      return toGraphQLException(validationException);
    }
    if (exception instanceof RuntimeException) {
      return (RuntimeException) exception;
    }
    return new RuntimeException(operationDescription, exception);
  }

  @Nonnull
  static RuntimeException toGraphQLException(@Nonnull ValidationException validationException) {
    ValidationExceptionCollection collection =
        validationException.getValidationExceptionCollection();
    if (collection != null) {
      AspectValidationException aspectException =
          collection.streamAllExceptions().findFirst().orElse(null);
      if (aspectException != null) {
        String message = aspectException.getMessage();
        if (isAuthorizationFailure(aspectException, message)) {
          return new AuthorizationException(
              message != null ? message : validationException.getMessage(), validationException);
        }
        return new com.linkedin.datahub.graphql.exception.ValidationException(
            message != null ? message : validationException.getMessage(), validationException);
      }
    }

    String message = validationException.getMessage();
    if (isUnauthorizedMessage(message)) {
      return new AuthorizationException(message, validationException);
    }
    return new com.linkedin.datahub.graphql.exception.ValidationException(
        message, validationException);
  }

  private static boolean isAuthorizationFailure(
      @Nonnull AspectValidationException aspectException, @Nullable String message) {
    return ValidationSubType.AUTHORIZATION.equals(aspectException.getSubType())
        || isUnauthorizedMessage(message);
  }

  private static boolean isUnauthorizedMessage(@Nullable String message) {
    return message != null && message.toLowerCase().contains("unauthorized");
  }

  @Nullable
  private static <T extends Throwable> T findCause(
      @Nullable Throwable throwable, @Nonnull Class<T> clazz) {
    while (throwable != null) {
      if (clazz.isInstance(throwable)) {
        return clazz.cast(throwable);
      }
      throwable = throwable.getCause();
    }
    return null;
  }
}
