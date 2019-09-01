package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.r2.message.RequestContext;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;


/**
 * A base class for rest.li auditors
 */
public abstract class BaseRestliAuditor {

  public static final Urn DEFAULT_ACTOR;
  static {
    try {
      DEFAULT_ACTOR = Urn.createFromString("urn:li:principal:UNKNOWN");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates an {@link AuditStamp} based on the current {@link RequestContext}.
   */
  @Nonnull
  public abstract AuditStamp requestAuditStamp(@Nonnull RequestContext requestContext);
}
