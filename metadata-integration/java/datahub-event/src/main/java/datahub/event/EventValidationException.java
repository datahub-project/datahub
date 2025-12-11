/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.event;

public class EventValidationException extends RuntimeException {
  public EventValidationException(String message) {
    super(message);
  }

  public EventValidationException(String message, Throwable t) {
    super(message, t);
  }
}
