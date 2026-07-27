package com.linkedin.gms.factory.ratelimit;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigValidator;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
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
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
// Loads the optional operator override (Tier 2); defaults live in application.yaml. Declared on the
// factory so it applies wherever the engine is built (GMS, datahub-upgrade).
// RATE_LIMITS_CONFIG_FILE
// is a Spring resource URI (e.g. file:/etc/datahub/rate-limits.yaml); when unset it resolves to the
// bundled empty rate-limit-config.yaml. ignoreResourceNotFound tolerates a set-but-missing path.
@PropertySource(
    name = "rateLimitConfigOverride",
    value = "${RATE_LIMITS_CONFIG_FILE:classpath:/rate-limit-config.yaml}",
    ignoreResourceNotFound = true,
    factory = YamlPropertySourceFactory.class)
public class RateLimitEngineFactory {

  static final String CONFIG_FILE_ENV = "RATE_LIMITS_CONFIG_FILE";
  // Removed with the legacy Jackson config loader (replaced by the Spring @PropertySource overlay).
  static final String CONFIG_FILE_ENABLED_ENV = "RATE_LIMITS_CONFIG_FILE_ENABLED";
  static final String CONFIG_JSON_ENV = "RATE_LIMITS_CONFIG_JSON";

  @Bean
  @Nonnull
  public RateLimitEngine rateLimitEngine(
      GMSConfiguration gmsConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      Environment environment) {
    // Spring binds the full config (application.yaml toggles + rate-limit-config.yaml policy, with
    // ${ENV} placeholders resolved and any mounted RATE_LIMITS_CONFIG_FILE overlaid). Validate the
    // bound bean up front so a bad config fails startup instead of silently mis-limiting traffic.
    warnIfConfigFileLikelyUnresolved(environment);
    warnIfRemovedEnvVarsSet(environment);
    RateLimitProperties config =
        gmsConfiguration.getRateLimits() != null
            ? gmsConfiguration.getRateLimits()
            : new RateLimitProperties();
    RateLimitConfigValidator.validate(config);
    String basePath =
        BasePathUtils.resolveBasePath(
            gmsConfiguration.getBasePathEnabled(), gmsConfiguration.getBasePath());
    return new RateLimitEngine(
        config,
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
   * RATE_LIMITS_CONFIG_FILE is consumed as a Spring resource URI by the @PropertySource overlay in
   * CommonApplicationConfig, which uses ignoreResourceNotFound — so a value without a resource
   * scheme (e.g. a bare {@code /etc/datahub/rate-limits.yaml} instead of {@code
   * file:/etc/datahub/rate-limits.yaml}) is silently skipped and the bundled defaults load instead.
   * Warn loudly so that misconfiguration isn't invisible (the old file loader logged on miss).
   */
  private static void warnIfConfigFileLikelyUnresolved(Environment environment) {
    String configFile = environment.getProperty(CONFIG_FILE_ENV);
    if (configFile == null || configFile.isBlank()) {
      return;
    }
    if (!configFile.startsWith("file:") && !configFile.startsWith("classpath:")) {
      log.warn(
          "{}='{}' has no Spring resource scheme; it will be ignored (overlay skipped) and the "
              + "bundled rate-limit defaults will load. Use a scheme, e.g. file:{}",
          CONFIG_FILE_ENV,
          configFile,
          configFile);
    }
  }

  /**
   * {@code RATE_LIMITS_CONFIG_FILE_ENABLED} and {@code RATE_LIMITS_CONFIG_JSON} were features of
   * the legacy Jackson config loader, which the Spring {@code @PropertySource} overlay replaced.
   * Neither is honored anymore: the mounted override is enabled simply by pointing {@code
   * RATE_LIMITS_CONFIG_FILE} at a file, and there is no inline-JSON overlay. Warn loudly so an
   * operator relying on either isn't silently mis-configured (a silent breaking change otherwise).
   */
  private static void warnIfRemovedEnvVarsSet(Environment environment) {
    warnRemoved(
        environment,
        CONFIG_FILE_ENABLED_ENV,
        "the mounted override now loads whenever "
            + CONFIG_FILE_ENV
            + " points at a file (presence = enabled); remove this variable");
    warnRemoved(
        environment,
        CONFIG_JSON_ENV,
        "inline-JSON overlays were removed with the legacy loader; move the settings into the "
            + "mounted "
            + CONFIG_FILE_ENV
            + " YAML file");
  }

  private static void warnRemoved(Environment environment, String envVar, String guidance) {
    String value = environment.getProperty(envVar);
    if (value != null && !value.isBlank()) {
      log.warn("{} is set but is no longer supported and will be ignored: {}.", envVar, guidance);
    }
  }
}
