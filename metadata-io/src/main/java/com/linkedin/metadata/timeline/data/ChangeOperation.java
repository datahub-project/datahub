package com.linkedin.metadata.timeline.data;

public enum ChangeOperation {
  /**
   * Something is added to an entity, e.g. tag, glossary term.
   */
  ADD,
  /**
   * An entity is modified. e.g. Domain, description is updated.
   */
  MODIFY,
  /**
   * Something is removed from an entity. e.g. tag, glossary term.
   */
  REMOVE,
  /**
   * Entity is created.
   */
  CREATE,
  /**
   * Entity is hard-deleted.
   */
  HARD_DELETE,
  /**
   * Entity is soft-deleted.
   */
  SOFT_DELETE,
  /**
   * Entity is reinstated after being soft-deleted.
   */
  REINSTATE,
}
