package com.linkedin.metadata.kafka.context.inbound;

import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring bean registration for the inbound-context extension points.
 *
 * <ul>
 *   <li>{@link InboundContextResolver} — composed from any {@link InboundContextEnricher} beans
 *       Spring discovers. When no enrichers are registered (default), Spring injects an empty list
 *       and the resolver becomes a pass-through.
 *   <li>{@link InboundBatchAffinityResolver} — single-bean extension point. The default {@link
 *       DefaultInboundBatchAffinityResolver} is registered here via
 *       {@code @ConditionalOnMissingBean} so it loads only when no other implementation is present.
 * </ul>
 */
@Configuration
public class InboundContextResolverFactory {

  @Bean
  @Nonnull
  public InboundContextResolver inboundContextResolver(
      @Nonnull final List<InboundContextEnricher> enrichers) {
    return new InboundContextResolver(enrichers);
  }

  @Bean
  @ConditionalOnMissingBean(InboundBatchAffinityResolver.class)
  @Nonnull
  public InboundBatchAffinityResolver defaultInboundBatchAffinityResolver() {
    return new DefaultInboundBatchAffinityResolver();
  }
}
