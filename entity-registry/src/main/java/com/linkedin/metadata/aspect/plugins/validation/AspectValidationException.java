package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.util.Pair;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AspectValidationException extends Exception {

  public static AspectValidationException forItem(BatchItem item, String msg) {
    return forItem(item, msg, null);
  }

  public static AspectValidationException forItem(BatchItem item, String msg, Exception e) {
    return new AspectValidationException(item.getUrn(), item.getAspectName(), msg, e);
  }

  @Nonnull private final Urn entityUrn;
  @Nonnull private final String aspectName;
  @Nullable private final String msg;

  public AspectValidationException(@Nonnull Urn entityUrn, @Nonnull String aspectName, String msg) {
    this(entityUrn, aspectName, msg, null);
  }

  public AspectValidationException(
      @Nonnull Urn entityUrn, @Nonnull String aspectName, @Nonnull String msg, Exception e) {
    super(msg, e);
    this.entityUrn = entityUrn;
    this.aspectName = aspectName;
    this.msg = msg;
  }

  public Pair<Urn, String> getExceptionKey() {
    return Pair.of(entityUrn, aspectName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AspectValidationException that = (AspectValidationException) o;

    if (!entityUrn.equals(that.entityUrn)) return false;
    if (!aspectName.equals(that.aspectName)) return false;
    return Objects.equals(msg, that.msg);
  }

  @Override
  public int hashCode() {
    int result = entityUrn.hashCode();
    result = 31 * result + aspectName.hashCode();
    result = 31 * result + (msg != null ? msg.hashCode() : 0);
    return result;
  }
}
