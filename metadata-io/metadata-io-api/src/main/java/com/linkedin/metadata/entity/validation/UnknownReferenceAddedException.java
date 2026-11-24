package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * A write rejected because it adds a reference to an entity type this version doesn't know. It is
 * found while the write is applied, so it names the aspect it rejects: a caller writing unrelated
 * proposals together can reject that one and write the rest.
 */
@Getter
public class UnknownReferenceAddedException extends ValidationException {
  @Nonnull private final Urn urn;
  @Nonnull private final String aspectName;

  public UnknownReferenceAddedException(
      @Nonnull final String message, @Nonnull final Urn urn, @Nonnull final String aspectName) {
    super(message);
    this.urn = urn;
    this.aspectName = aspectName;
  }
}
