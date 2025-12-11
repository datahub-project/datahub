/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
