/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
    return new AspectValidationException(item, msg, ValidationSubType.VALIDATION, e);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg) {
    return forPrecondition(item, msg, null);
  }

  public static AspectValidationException forFilter(BatchItem item, String msg) {
    return new AspectValidationException(item, msg, ValidationSubType.FILTER);
  }

  public static AspectValidationException forAuth(BatchItem item, String msg) {
    return new AspectValidationException(item, msg, ValidationSubType.AUTHORIZATION);
  }

  public static AspectValidationException forPrecondition(BatchItem item, String msg, Exception e) {
    return new AspectValidationException(item, msg, ValidationSubType.PRECONDITION, e);
  }

  @Nonnull BatchItem item;
  @Nonnull ChangeType changeType;
  @Nonnull Urn entityUrn;
  @Nonnull String aspectName;
  @Nonnull ValidationSubType subType;
  @Nullable String msg;

  public AspectValidationException(@Nonnull BatchItem item, String msg, ValidationSubType subType) {
    this(item, msg, subType, null);
  }

  public AspectValidationException(
      @Nonnull BatchItem item,
      @Nonnull String msg,
      @Nullable ValidationSubType subType,
      Exception e) {
    super(msg, e);
    this.item = item;
    this.changeType = item.getChangeType();
    this.entityUrn = item.getUrn();
    this.aspectName = item.getAspectName();
    this.msg = msg;
    this.subType = subType != null ? subType : ValidationSubType.VALIDATION;
  }

  public Pair<Urn, String> getAspectGroup() {
    return Pair.of(entityUrn, aspectName);
  }
}
