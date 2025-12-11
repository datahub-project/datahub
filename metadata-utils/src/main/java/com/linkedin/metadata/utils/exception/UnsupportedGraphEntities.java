/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.utils.exception;

/** An exception to be thrown when certain graph entities are not supported. */
public class UnsupportedGraphEntities extends RuntimeException {

  public UnsupportedGraphEntities(String message) {
    super(message);
  }

  public UnsupportedGraphEntities(String message, Throwable throwable) {
    super(message, throwable);
  }
}
