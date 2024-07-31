package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.util.Pair;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = false)
public class AspectValidationException extends Exception {

  public static AspectValidationException forItem(BatchItem item, String msg) {
    return forItem(item, msg, null);
  }

  public static AspectValidationException forItem(BatchItem item, String msg, Exception e) {
    return new AspectValidationException(
        item.getChangeType(), item.getUrn(), item.getAspectName(), msg, SubType.VALIDATION, e);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg) {
    return forPrecondition(item, msg, null);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg, Exception e) {
    return new AspectValidationException(
        item.getChangeType(), item.getUrn(), item.getAspectName(), msg, SubType.PRECONDITION, e);
  }

  @Nonnull ChangeType changeType;
  @Nonnull Urn entityUrn;
  @Nonnull String aspectName;
  @Nonnull SubType subType;
  @Nullable String msg;

  public AspectValidationException(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      String msg,
      SubType subType) {
    this(changeType, entityUrn, aspectName, msg, subType, null);
  }

  public AspectValidationException(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull String msg,
      @Nullable SubType subType,
      Exception e) {
    super(msg, e);
    this.changeType = changeType;
    this.entityUrn = entityUrn;
    this.aspectName = aspectName;
    this.msg = msg;
    this.subType = subType != null ? subType : SubType.VALIDATION;
  }

  public Pair<Urn, String> getAspectGroup() {
    return Pair.of(entityUrn, aspectName);
  }

  public static enum SubType {
    VALIDATION,
    PRECONDITION
  }
}
