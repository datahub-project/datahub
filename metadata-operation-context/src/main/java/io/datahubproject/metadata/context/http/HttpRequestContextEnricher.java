package io.datahubproject.metadata.context.http;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Produces HTTP headers derived from the operation context, to be attached to outbound HTTP
 * requests.
 *
 * <p>The SPI is transport-agnostic on purpose: it returns a header map rather than mutating a
 * concrete request type, so the same enricher can decorate every outbound stack DataHub uses
 * (Apache HttpClient, the JDK {@code java.net.http} client, and the generated okHttp client).
 * Multiple enrichers are composed via {@link HttpRequestContextResolver}.
 *
 * <p>Use cases: deployment-specific routing headers, tracing IDs, security tokens, etc.
 */
public interface HttpRequestContextEnricher {

  /**
   * Compute the headers this enricher wants attached to an outbound request.
   *
   * @param operationContext The operation context to derive header values from
   * @return header name → value pairs; empty when this enricher contributes nothing
   */
  @Nonnull
  Map<String, String> headers(@Nonnull final OperationContext operationContext);
}
