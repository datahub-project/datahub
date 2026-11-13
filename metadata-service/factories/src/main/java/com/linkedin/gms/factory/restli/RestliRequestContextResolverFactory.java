package com.linkedin.gms.factory.restli;

import com.linkedin.common.client.restli.RestliRequestContextEnricher;
import com.linkedin.common.client.restli.RestliRequestContextResolver;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link RestliRequestContextResolver} bean, composing any {@link
 * RestliRequestContextEnricher} implementations Spring discovers. When no enrichers are registered
 * Spring injects an empty list and the resolver becomes a pass-through; deployments that need
 * outbound decoration contribute enrichers that stamp headers on every outbound Restli call.
 *
 * <p>Mirror of {@link com.linkedin.gms.factory.http.HttpRequestContextResolverFactory} for the
 * Restli call surface.
 */
@Configuration
public class RestliRequestContextResolverFactory {

  @Bean
  @Nonnull
  public RestliRequestContextResolver restliRequestContextResolver(
      final List<RestliRequestContextEnricher> enrichers) {
    return new RestliRequestContextResolver(enrichers);
  }
}
