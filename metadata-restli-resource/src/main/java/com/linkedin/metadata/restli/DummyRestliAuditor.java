package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.r2.message.RequestContext;
import java.time.Clock;
import javax.annotation.Nonnull;


public class DummyRestliAuditor extends BaseRestliAuditor {

  private final Clock _clock;

  public DummyRestliAuditor(@Nonnull Clock clock) {
    _clock = clock;
  }

  @Override
  @Nonnull
  public AuditStamp requestAuditStamp(@Nonnull RequestContext requestContext) {
    return new AuditStamp().setTime(_clock.millis()).setActor(DEFAULT_ACTOR);
  }
}
