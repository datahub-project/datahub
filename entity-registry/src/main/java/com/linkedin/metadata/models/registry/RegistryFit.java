package com.linkedin.metadata.models.registry;

/**
 * How a piece of data (a row, event, request or reference) fits this version's {@link
 * EntityRegistry}. See {@link RegistryKnowledge}.
 */
public enum RegistryFit {
  /** The entity type, and the aspect if one is named, are in the registry. */
  KNOWN,
  /** The entity type is not in the registry, e.g. it was added by a newer version. */
  UNKNOWN_ENTITY_TYPE,
  /** The entity type is known but the aspect is not, e.g. it was added by a newer version. */
  UNKNOWN_ASPECT,
  /**
   * The input can't be classified (missing entity type, unparseable urn). This is a data error, not
   * version drift, so callers leave it to their existing validation rather than skipping it.
   */
  MALFORMED;

  /** True for data a newer version may have written that this version should skip. */
  public boolean isUnknown() {
    return this == UNKNOWN_ENTITY_TYPE || this == UNKNOWN_ASPECT;
  }
}
