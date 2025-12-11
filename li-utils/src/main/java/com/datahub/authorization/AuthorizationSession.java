/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Combines a common interface for actor and authorizer which is cached per session */
public interface AuthorizationSession {
  AuthorizationResult authorize(
      @Nonnull final String privilege, @Nullable final EntitySpec resourceSpec);

  AuthorizationResult authorize(
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources);
}
