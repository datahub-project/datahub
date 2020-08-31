package com.linkedin.metadata.utils;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public final class AuditStamps {
  private AuditStamps() {
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull Urn actorUrn, @Nullable Urn impersonatorUrn, long time) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(time);
    auditStamp.setActor(actorUrn);
    if (impersonatorUrn != null) {
      auditStamp.setImpersonator(impersonatorUrn);
    }
    return auditStamp;
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull String actorLdap, long time) {
    try {
      return makeAuditStamp(new Urn("urn:li:testActor:" + actorLdap), null, time);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull String actorLdap) {
    return makeAuditStamp(actorLdap, 0L);
  }
}
