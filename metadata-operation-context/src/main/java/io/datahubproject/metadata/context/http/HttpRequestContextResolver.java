package io.datahubproject.metadata.context.http;

import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves outbound-HTTP context enrichment headers.
 *
 * <p>Single chokepoint for all outbound-HTTP context decoration. Composes every registered {@link
 * HttpRequestContextEnricher} into a single header map, in injection order.
 *
 * <p>Transport-agnostic by design: it returns headers rather than mutating a concrete request type,
 * so callers on any HTTP stack (Apache HttpClient, the JDK {@code java.net.http} client, the
 * generated okHttp client) apply them natively. Registered as a Spring bean via {@code
 * HttpRequestContextResolverFactory}. When no enrichers are registered (OSS build) it resolves to
 * an empty map — a pass-through.
 */
@Slf4j
public class HttpRequestContextResolver {

  private final List<HttpRequestContextEnricher> enrichers;

  public HttpRequestContextResolver(@Nonnull final List<HttpRequestContextEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  /**
   * Compose all registered enrichers' headers for the given context, in injection order. Later
   * enrichers override earlier ones on a header-name collision.
   *
   * @param operationContext The operation context to derive header values from
   * @return merged header name → value pairs; empty when no enrichers are registered
   */
  @Nonnull
  public Map<String, String> resolveHeaders(@Nonnull final OperationContext operationContext) {
    final Map<String, String> headers = new LinkedHashMap<>();
    for (HttpRequestContextEnricher enricher : enrichers) {
      headers.putAll(enricher.headers(operationContext));
    }
    return headers;
  }
}
