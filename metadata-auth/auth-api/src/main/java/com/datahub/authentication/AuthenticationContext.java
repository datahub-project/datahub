/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authentication;

/**
 * A static wrapper around a {@link ThreadLocal} instance of {@link Authentication} containing
 * information about the currently authenticated actor.
 */
public class AuthenticationContext {
  private static final ThreadLocal<Authentication> AUTHENTICATION = new ThreadLocal<>();

  public static Authentication getAuthentication() {
    return AUTHENTICATION.get();
  }

  public static void setAuthentication(Authentication authentication) {
    AUTHENTICATION.set(authentication);
  }

  public static void remove() {
    AUTHENTICATION.remove();
  }

  private AuthenticationContext() {}
}
