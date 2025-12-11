/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models;

/** Exception thrown when Entity, Aspect models fail to be validated. */
public class ModelValidationException extends RuntimeException {

  public ModelValidationException(String message) {
    super(message);
  }

  public ModelValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ModelValidationException(Throwable cause) {
    super(cause);
  }
}
