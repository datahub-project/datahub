package com.datahub.authorization;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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

  /** {@link #resolve(EntitySpec, OperationContext)} for many specs at once. */
  @Nonnull
  default Map<EntitySpec, ResolvedEntitySpec> resolveAll(
      @Nonnull Collection<EntitySpec> entitySpecs, @Nonnull OperationContext opContext) {
    final Map<EntitySpec, ResolvedEntitySpec> resolved = new HashMap<>();
    entitySpecs.forEach(spec -> resolved.put(spec, resolve(spec, opContext)));
    return resolved;
  }
}
