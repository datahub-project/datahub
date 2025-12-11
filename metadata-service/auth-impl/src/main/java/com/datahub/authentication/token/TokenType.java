/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication.token;

/** Represents a type of JWT access token granted by the {@link StatelessTokenService}. */
public enum TokenType {

  /** A UI-initiated session token */
  SESSION,
  /** A personal token for programmatic use */
  PERSONAL;
}
