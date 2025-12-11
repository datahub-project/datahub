/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.util.exception;

/** An exception to be thrown when Model Conversion fails. */
public class ModelConversionException extends RuntimeException {

  public ModelConversionException(String message) {
    super(message);
  }

  public ModelConversionException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
