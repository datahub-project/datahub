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
    this(validationExceptionCollection.toString());
    this.validationExceptionCollection = validationExceptionCollection;
  }
}
