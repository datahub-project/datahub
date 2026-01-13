package com.linkedin.gms.factory.throttle;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.throttle.RateLimitThrottleSensor;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimitThrottleFactory {
  @Bean("rateLimitThrottle")
  public ThrottleSensor rateLimitThrottle(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      @Nullable @Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
    MetadataChangeProposalConfig.RateLimitConfig rateLimitConfig =
        provider.getMetadataChangeProposal().getThrottle().getRateLimit();

    if (!rateLimitConfig.isEnabled() || rateLimitConfig.getUpdateIntervalMs() <= 0) {
      return new NoOpSensor();
    }
    return RateLimitThrottleSensor.builder()
        .activationQuota(rateLimitConfig.getActivationQuota())
        .activationIntervalMinutes(rateLimitConfig.getActivationIntervalMinutes())
        .rateLimitIntervalSeconds(rateLimitConfig.getRateLimitIntervalSeconds())
        .limitPerInterval(rateLimitConfig.getLimitPerInterval())
        .updateIntervalMs(rateLimitConfig.getUpdateIntervalMs())
        .jitterRatio(rateLimitConfig.getJitterRatio())
        .hazelcastInstance(hazelcastInstance)
        .build()
        .start();
  }
}
