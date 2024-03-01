package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import javax.annotation.Nonnull;

public class ServiceUtils {

  public static AuditStamp createSystemAuditStamp() {
    return createAuditStamp(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  public static AuditStamp createAuditStamp(@Nonnull final Authentication authentication) {
    return createAuditStamp(UrnUtils.getUrn(authentication.getActor().toUrnStr()));
  }

  public static AuditStamp createAuditStamp(@Nonnull final Urn actor) {
    return new AuditStamp().setTime(System.currentTimeMillis()).setActor(actor);
  }
}
