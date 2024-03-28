package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTimeUtils;

@Slf4j
public class AuditStampUtils {
  private AuditStampUtils() {}

  public static AuditStamp createDefaultAuditStamp() {
    return getAuditStamp(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  public static AuditStamp createAuditStamp(@Nonnull String actorUrn) {
    return getAuditStamp(UrnUtils.getUrn(actorUrn));
  }

  public static AuditStamp getAuditStamp(Urn actor) {
    return getAuditStamp(actor, null);
  }

  public static AuditStamp getAuditStamp(@Nonnull Urn actor, @Nullable Long currentTimeMs) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(currentTimeMs != null ? currentTimeMs : DateTimeUtils.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
  }
}
