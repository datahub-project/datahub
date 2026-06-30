package com.linkedin.gms.factory.ratelimit;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigValidator;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import com.linkedin.metadata.utils.BasePathUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimitEngineFactory {

  @Bean
  @Nonnull
  public RateLimitEngine rateLimitEngine(
      GMSConfiguration gmsConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    // Spring binds the full config (application.yaml toggles + rate-limit-config.yaml policy, with
    // ${ENV} placeholders resolved and any mounted RATE_LIMITS_CONFIG_FILE overlaid). Validate the
    // bound bean up front so a bad config fails startup instead of silently mis-limiting traffic.
    RateLimitProperties config =
        gmsConfiguration.getRateLimits() != null
            ? gmsConfiguration.getRateLimits()
            : new RateLimitProperties();
    RateLimitConfigValidator.validate(config);
    String basePath =
        BasePathUtils.resolveBasePath(
            gmsConfiguration.getBasePathEnabled(), gmsConfiguration.getBasePath());
    return new RateLimitEngine(
        config, basePath, meterRegistry, hazelcastInstance, systemOperationContext.getObjectMapper());
  }

  @Bean
  @Nonnull
  public RateLimitFilter rateLimitFilter(RateLimitEngine rateLimitEngine) {
    return new RateLimitFilter(rateLimitEngine);
  }
}
