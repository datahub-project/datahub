package com.linkedin.common.client.restli;

import com.linkedin.restli.client.AbstractRequestBuilder;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Enriches outbound Restli requests with headers derived from the operation context.
 *
 * <p>The Restli analogue of {@link
 * io.datahubproject.metadata.context.http.HttpRequestContextEnricher}. Restli uses its own {@link
 * AbstractRequestBuilder} rather than Apache {@code HttpUriRequest}, so a separate interface is
 * required even though the contract is identical: mutate the builder (usually by adding headers)
 * based on data carried in the {@link OperationContext}.
 *
 * <p>Implementations are composed by {@link RestliRequestContextResolver} and applied at every
 * outbound Restli call site (see {@link com.linkedin.common.client.BaseClient#sendClientRequest}).
 *
 * <p>Typical use cases: deployment-specific routing headers, tracing tokens, security tokens, etc.
 * When no enrichers are registered the outbound call path is unchanged; deployments register
 * enrichers as needed to stamp headers the receiving GMS can act on.
 */
public interface RestliRequestContextEnricher {

  /**
   * Mutate the supplied request builder to attach any headers required by this enricher.
   *
   * <p>For single-valued headers (routing, auth, tracing tokens) implementations should use {@code
   * setHeader} rather than {@code addHeader}: {@code addHeader} appends, so when multiple enrichers
   * touch the same header name the values would be comma-joined instead of last-writer-wins.
   *
   * @param requestBuilder The outbound Restli request to mutate
   * @param operationContext The operation context to derive header values from
   */
  void enrich(
      @Nonnull final AbstractRequestBuilder<?, ?, ?> requestBuilder,
      @Nonnull final OperationContext operationContext);
}
