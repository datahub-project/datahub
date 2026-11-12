package com.linkedin.metadata.service.docimport;

import javax.annotation.Nullable;

/**
 * Internal use case that determines document subtype and defaults. This is set by the calling code
 * (frontend/API), not chosen by end users.
 */
public enum ImportUseCase {
  CONTEXT_DOCUMENT,
  SKILL;

  @Nullable
  public String toSubType() {
    if (this == SKILL) {
      return "SKILL";
    }
    return null;
  }

  /** Parse from a string, defaulting to CONTEXT_DOCUMENT for unknown values. */
  public static ImportUseCase fromString(@Nullable String value) {
    if (value == null) {
      return CONTEXT_DOCUMENT;
    }
    try {
      return valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      return CONTEXT_DOCUMENT;
    }
  }
}
