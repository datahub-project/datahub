package com.linkedin.metadata.kafka.context.inbound;

import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link InboundContextResolver} bean composing any {@link InboundContextEnricher}
 * implementations Spring discovers. When no enrichers are registered (the OSS default), Spring
 * injects an empty list and the resolver becomes a pass-through.
 */
@Configuration
public class InboundContextResolverFactory {

  @Bean
  @Nonnull
  public InboundContextResolver inboundContextResolver(
      @Nonnull final List<InboundContextEnricher> enrichers) {
    return new InboundContextResolver(enrichers);
  }
}
