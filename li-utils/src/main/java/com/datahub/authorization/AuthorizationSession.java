package com.datahub.authorization;

import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Combines a common interface for actor and authorizer which is cached per session */
public interface AuthorizationSession {
  BatchAuthorizationResult authorize(
      @Nonnull Set<String> privileges,
      @Nullable EntitySpec resourceSpec,
      @Nonnull Collection<EntitySpec> subResources);
}
