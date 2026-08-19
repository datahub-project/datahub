package com.linkedin.metadata.dao.producer.context.outbound;

import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link OutboundContextResolver} bean composing any {@link OutboundContextEnricher}
 * implementations Spring discovers. When no enrichers are registered (the OSS default), Spring
 * injects an empty list and the resolver does nothing on {@code apply()}.
 */
@Configuration
public class OutboundContextResolverFactory {

  @Bean
  @Nonnull
  public OutboundContextResolver outboundContextResolver(
      @Nonnull final List<OutboundContextEnricher> enrichers) {
    return new OutboundContextResolver(enrichers);
  }
}
