package com.datahub.authorization;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Extension of {@link EntitySpecResolver} that resolves actor-scoped fields (e.g. group membership)
 * using the session {@link OperationContext}. Resource metadata reads during policy evaluation may
 * still use the system context via {@link #resolve(EntitySpec)}.
 */
public interface ContextualEntitySpecResolver extends EntitySpecResolver {

  /** Resolve a {@link EntitySpec} using the provided operation context for field resolvers. */
  @Nonnull
  ResolvedEntitySpec resolve(@Nonnull EntitySpec entitySpec, @Nonnull OperationContext opContext);
}
