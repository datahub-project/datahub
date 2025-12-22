package com.linkedin.metadata.entity.validation;

/** Identifies where in the processing pipeline aspect validation occurred. */
public enum ValidationPoint {
  /** Validation of existing aspect from database before applying patches to it. */
  PRE_DB_PATCH("prePatch"),

  /** Validation after applying patches, before writing updated aspect to database. */
  POST_DB_PATCH("postPatch");

  private final String displayName;

  ValidationPoint(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public String toString() {
    return displayName;
  }
}
