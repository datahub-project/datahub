package com.linkedin.gms.factory.http;

import io.datahubproject.metadata.context.http.HttpRequestContextEnricher;
import io.datahubproject.metadata.context.http.HttpRequestContextResolver;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link HttpRequestContextResolver} bean, composing any {@link
 * HttpRequestContextEnricher} implementations Spring discovers. When no enrichers are registered,
 * Spring injects an empty list and the resolver becomes a pass-through.
 */
@Configuration
public class HttpRequestContextResolverFactory {

  @Bean
  @Nonnull
  public HttpRequestContextResolver httpRequestContextResolver(
      final List<HttpRequestContextEnricher> enrichers) {
    return new HttpRequestContextResolver(enrichers);
  }
}
