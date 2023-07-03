package com.datahub.authorization;

/**
 * A Resource Spec Resolver is responsible for resolving a {@link ResourceSpec} to a {@link ResolvedResourceSpec}.
 */
public interface ResourceSpecResolver {
  /**
   Resolve a {@link ResourceSpec} to a resolved resource spec.
   **/
  ResolvedResourceSpec resolve(ResourceSpec resourceSpec);
}
