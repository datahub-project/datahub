/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import lombok.AllArgsConstructor;
import lombok.Data;

/** A result returned after requesting authorization for a particular privilege. */
@Data
@AllArgsConstructor
public class AuthorizationResult {
  /** The original authorization request */
  AuthorizationRequest request;

  /** The result type. Allow or deny the authorization request for the actor. */
  public enum Type {
    /** Allow the request - the requested actor is privileged. */
    ALLOW,
    /** Deny the request - the requested actor is not privileged. */
    DENY
  }

  /** The decision - whether to allow or deny the request. */
  public Type type;

  /** Optional message associated with the decision. Useful for debugging. */
  String message;
}
