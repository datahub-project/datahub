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
    return new AspectValidationException(item, msg, SubType.VALIDATION, e);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg) {
    return forPrecondition(item, msg, null);
  }

  public static AspectValidationException forFilter(BatchItem item, String msg) {
    return new AspectValidationException(item, msg, SubType.FILTER);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg, Exception e) {
    return new AspectValidationException(item, msg, SubType.PRECONDITION, e);
  }

  @Nonnull BatchItem item;
  @Nonnull ChangeType changeType;
  @Nonnull Urn entityUrn;
  @Nonnull String aspectName;
  @Nonnull SubType subType;
  @Nullable String msg;

  public AspectValidationException(@Nonnull BatchItem item, String msg, SubType subType) {
    this(item, msg, subType, null);
  }

  public AspectValidationException(
      @Nonnull BatchItem item, @Nonnull String msg, @Nullable SubType subType, Exception e) {
    super(msg, e);
    this.item = item;
    this.changeType = item.getChangeType();
    this.entityUrn = item.getUrn();
    this.aspectName = item.getAspectName();
    this.msg = msg;
    this.subType = subType != null ? subType : SubType.VALIDATION;
  }

  public Pair<Urn, String> getAspectGroup() {
    return Pair.of(entityUrn, aspectName);
  }

  public enum SubType {
    // A validation exception is thrown
    VALIDATION,
    // A failed precondition is thrown if the header constraints are not met
    PRECONDITION,
    // Exclude from processing further
    FILTER
  }
}
