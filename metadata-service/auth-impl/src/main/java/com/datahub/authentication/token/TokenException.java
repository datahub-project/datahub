/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication.token;

/** A checked exception that is thrown when a DataHub-issued access token cannot be verified. */
public class TokenException extends Exception {

  public TokenException(final String message) {
    super(message);
  }

  public TokenException(final String message, final Throwable e) {
    super(message, e);
  }
}
