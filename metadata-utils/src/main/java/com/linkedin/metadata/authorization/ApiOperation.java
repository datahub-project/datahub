/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.authorization;

import com.linkedin.events.metadata.ChangeType;

public enum ApiOperation {
  CREATE,
  READ,
  UPDATE,
  DELETE,
  EXISTS,
  EXECUTE,
  /**
   * Manage is a composite of all privileges which can be reduced to UPDATE (CREATE, READ, EXISTS)
   * and DELETE in the case where there is not an explicit MANAGE privilege
   */
  MANAGE,
  ;

  public static ApiOperation fromChangeType(ChangeType type) {
    switch (type) {
      case PATCH:
      case UPDATE:
      case UPSERT:
      case RESTATE:
        return UPDATE;
      case CREATE:
      case CREATE_ENTITY:
        return CREATE;
      case DELETE:
        return DELETE;
      default:
        // If type cannot be determined, use MANAGE as it is composite of all operations
        return MANAGE;
    }
  }
}
