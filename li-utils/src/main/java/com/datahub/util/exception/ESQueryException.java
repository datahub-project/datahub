/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.util.exception;

/** An exception to be thrown when elastic search query fails. */
public class ESQueryException extends RuntimeException {

  public ESQueryException(String message) {
    super(message);
  }

  public ESQueryException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
