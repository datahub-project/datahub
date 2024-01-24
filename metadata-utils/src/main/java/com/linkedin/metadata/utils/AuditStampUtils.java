package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URISyntaxException;
import java.time.Clock;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditStampUtils {
  private AuditStampUtils() {}

  public static AuditStamp createDefaultAuditStamp() {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
        .setTime(Clock.systemUTC().millis());
  }

  public static AuditStamp createAuditStamp(@Nonnull String actorUrn) throws URISyntaxException {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(Urn.createFromString(actorUrn));
    auditStamp.setTime(Clock.systemUTC().millis());
    return auditStamp;
  }
}
