package com.linkedin.gms.factory.ratelimit;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigLoader;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigValidator;
import com.linkedin.metadata.config.ratelimit.RateLimitEffectiveConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import com.linkedin.metadata.utils.BasePathUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
public class RateLimitEngineFactory {

  static final String CONFIG_FILE_ENABLED_ENV = "RATE_LIMITS_CONFIG_FILE_ENABLED";

  @Bean
  @Nonnull
  public RateLimitProperties effectiveRateLimitProperties(
      GMSConfiguration gmsConfiguration, Environment environment) {
    RateLimitProperties fromSpring =
        gmsConfiguration.getRateLimits() != null
            ? gmsConfiguration.getRateLimits()
            : new RateLimitProperties();
    return RateLimitEffectiveConfig.get(environment, fromSpring);
  }

  @Bean
  @Nonnull
  public RateLimitEngine rateLimitEngine(
      GMSConfiguration gmsConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      Environment environment,
      @Qualifier("effectiveRateLimitProperties") RateLimitProperties effectiveRateLimitProperties) {
    warnIfRemovedEnvVarsSet(environment);
    RateLimitConfigValidator.validate(effectiveRateLimitProperties);
    String basePath =
        BasePathUtils.resolveBasePath(
            gmsConfiguration.getBasePathEnabled(), gmsConfiguration.getBasePath());
    return new RateLimitEngine(
        effectiveRateLimitProperties,
        basePath,
        meterRegistry,
        hazelcastInstance,
        systemOperationContext.getObjectMapper());
  }

  @Bean
  @Nonnull
  public RateLimitFilter rateLimitFilter(
      RateLimitEngine rateLimitEngine,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    // Capture the system principal's URN once so per-request resolution can exempt it (its
    // high-volume internal calls shouldn't be per-actor throttled — mirrors the GraphQL gate).
    String systemActorUrn = systemOperationContext.getAuthentication().getActor().toUrnStr();
    return new RateLimitFilter(rateLimitEngine, () -> resolveRestActorUrn(systemActorUrn));
  }

  /**
   * The current REST request's rate-limit actor URN for the scoped per-actor bucket: the
   * authenticated caller's URN, or null for the exempt system principal or an unauthenticated
   * request. Read from {@link AuthenticationContext}, which the auth extraction filter (ordered
   * before the rate-limit filter) has already populated. Mirrors the GraphQL controller's handling.
   */
  @Nullable
  private static String resolveRestActorUrn(@Nonnull String systemActorUrn) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null || authentication.getActor() == null) {
      return null;
    }
    String actorUrn = authentication.getActor().toUrnStr();
    return actorUrn.equals(systemActorUrn) ? null : actorUrn;
  }

  /**
   * {@code RATE_LIMITS_CONFIG_FILE_ENABLED} was a flag on the original Jackson loader. Presence of
   * {@code RATE_LIMITS_CONFIG_FILE} now means "replace the classpath policy file". Warn if the
   * removed flag is still set so the change isn't silent.
   */
  private static void warnIfRemovedEnvVarsSet(Environment environment) {
    String value = environment.getProperty(CONFIG_FILE_ENABLED_ENV);
    if (value != null && !value.isBlank()) {
      log.warn(
          "{} is set but is no longer supported and will be ignored: the mounted override now"
              + " loads whenever {} points at a file (presence = enabled); remove this variable.",
          CONFIG_FILE_ENABLED_ENV,
          RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV);
    }
  }
}
