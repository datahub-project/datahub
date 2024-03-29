package com.linkedin.metadata.authorization;

public enum ApiOperation {
  CREATE,
  READ,
  UPDATE,
  DELETE,
  EXISTS,
  /**
   * Manage is a composite of all privileges which can be reduced to UPDATE (CREATE, READ, EXISTS)
   * and DELETE in the case where there is not an explicit MANAGE privilege
   */
  MANAGE
}
