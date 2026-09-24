package com.linkedin.common.client.restli;

import com.linkedin.restli.client.AbstractRequestBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves and applies Restli request context enrichment.
 *
 * <p>Single chokepoint for all outbound-Restli context decoration. Composes all registered {@link
 * RestliRequestContextEnricher} instances in injection order.
 *
 * <p>The Restli analogue of {@link
 * io.datahubproject.metadata.context.http.HttpRequestContextResolver}. Lives in {@code
 * restli-client-api} (same module as {@link com.linkedin.common.client.BaseClient}) because the
 * interface refers to {@link AbstractRequestBuilder}, which is a Restli type — putting it in {@code
 * metadata-operation-context} would force that module to depend on Restli.
 *
 * <p>Registered as a Spring bean via the same factory pattern as {@code
 * HttpRequestContextResolverFactory}. Injected by {@link com.linkedin.common.client.BaseClient} so
 * every outbound Restli call goes through the same decoration path. When no enrichers are
 * registered (OSS build) the resolver is a pass-through — no behavioral change.
 */
@Slf4j
public class RestliRequestContextResolver {

  private final List<RestliRequestContextEnricher> enrichers;

  public RestliRequestContextResolver(@Nonnull final List<RestliRequestContextEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  /**
   * Apply all registered enrichers to the outbound request builder, in injection order.
   *
   * @param requestBuilder The outbound Restli request to enrich
   * @param operationContext The operation context to derive header values from
   */
  public void resolve(
      @Nonnull final AbstractRequestBuilder<?, ?, ?> requestBuilder,
      @Nonnull final OperationContext operationContext) {
    for (RestliRequestContextEnricher enricher : enrichers) {
      enricher.enrich(requestBuilder, operationContext);
    }
  }
}
