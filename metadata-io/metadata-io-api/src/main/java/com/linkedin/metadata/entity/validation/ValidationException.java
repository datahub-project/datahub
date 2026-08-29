package com.linkedin.metadata.entity.validation;

import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/** Exception thrown when a metadata record cannot be validated against its schema. */
@Getter
public class ValidationException extends RuntimeException {
  @Nullable private ValidationExceptionCollection validationExceptionCollection;

  public ValidationException(final String message) {
    super(message);
  }

  public ValidationException(@Nonnull ValidationExceptionCollection validationExceptionCollection) {
    // Keep the collection attached so API layers can map subtypes (e.g. AUTHORIZATION → 403).
    this(messageFor(validationExceptionCollection));
    this.validationExceptionCollection = validationExceptionCollection;
  }

  /**
   * Prefer everything the validators actually said, falling back to the previous diagnostic form so
   * this is never blank - GlobalControllerExceptionHandler puts it straight into a REST response.
   * The GraphQL/UI path deliberately does not read this; it asks the collection for its user-facing
   * text instead, so the fallback below can never reach a toast.
   */
  // Message for logs and REST responses; the UI reads the collection's own text instead.
  private static String messageFor(@Nonnull ValidationExceptionCollection collection) {
    String collectiveMessage = collection.getCollectiveMessageIncludingFiltered();
    return !collectiveMessage.isEmpty()
        ? collectiveMessage
        : "Failed to validate MCP due to: " + collection;
  }
}
