package io.datahubproject.metadata.context.usage.instrumentation;

import com.datahub.authentication.Authentication;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import javax.annotation.Nonnull;

/**
 * Optional hook invoked from {@link OperationContext#asSession} to attach usage-metric fields
 * before {@link RequestContext} is built, and to record request-phase metrics after the session
 * context is constructed.
 */
public interface SessionContextEnricher {

  /**
   * Populate usage-metric fields on the {@link RequestContext} builder (identity, auth channel,
   * byte measurement scratch space, etc.).
   */
  void enrichBeforeBuild(
      @Nonnull RequestContext.RequestContextBuilder requestBuilder,
      @Nonnull Authentication sessionAuthentication);

  /** Record request-phase counters after the session {@link OperationContext} is available. */
  void onSessionReady(@Nonnull OperationContext sessionContext);
}
