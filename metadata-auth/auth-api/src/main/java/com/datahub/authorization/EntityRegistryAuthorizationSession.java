package com.datahub.authorization;

import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;

/** Authorization session that can resolve registry {@link EntitySpec} metadata for auth checks. */
public interface EntityRegistryAuthorizationSession extends AuthorizationSession {

  @Nonnull
  EntityRegistry getEntityRegistry();
}
