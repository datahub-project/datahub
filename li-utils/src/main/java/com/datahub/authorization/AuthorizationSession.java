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
