package com.linkedin.metadata.testing;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
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
    return makeAuditStamp(new CorpuserUrn(actorLdap), null, time);
  }

  @Nonnull
  public static AuditStamp makeAuditStamp(@Nonnull String actorLdap) {
    return makeAuditStamp(actorLdap, 0L);
  }
}
