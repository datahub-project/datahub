package com.datahub.authorization;

/**
 * An Entity Spec Resolver is responsible for resolving a {@link EntitySpec} to a {@link
 * ResolvedEntitySpec}.
 */
public interface EntitySpecResolver {
  /** Resolve a {@link EntitySpec} to a resolved entity spec. */
  ResolvedEntitySpec resolve(EntitySpec entitySpec);
}
