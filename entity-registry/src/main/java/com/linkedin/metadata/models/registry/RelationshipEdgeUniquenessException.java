package com.linkedin.metadata.models.registry;

/** Thrown when two aspects claim the same directed relationship edge signature. */
public class RelationshipEdgeUniquenessException extends EntityRegistryException {
  public RelationshipEdgeUniquenessException(String message) {
    super(message);
  }
}
