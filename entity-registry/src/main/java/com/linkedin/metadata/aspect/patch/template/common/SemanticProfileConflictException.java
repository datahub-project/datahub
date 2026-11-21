package com.linkedin.metadata.aspect.patch.template.common;

import javax.annotation.Nonnull;
import lombok.Getter;

/** A profile precondition in a {@code semanticContent} patch did not match the stored aspect. */
@Getter
public class SemanticProfileConflictException extends RuntimeException {

  private final String path;

  public SemanticProfileConflictException(@Nonnull String path) {
    super("semanticContent precondition failed at " + path);
    this.path = path;
  }
}
