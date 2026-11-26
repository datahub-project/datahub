package com.linkedin.metadata.config.ratelimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

/**
 * Loads GMS rate-limit policy once from the Spring {@link Environment} and reuses it.
 *
 * <p>{@code @Conditional} Hazelcast bootstrap cannot wait for {@code RateLimitEngine}: it needs the
 * overlay-merged {@code endpoint.enabled} / {@code scoped.enabled} flags before those beans exist.
 * Binding into the {@code Environment} as property sources would revive the empty {@code rules: []}
 * list-ownership bug, so this holds the POJO instead. The engine factory reads the same instance.
 *
 * <p>Cached per {@code Environment} identity so tests with a fresh {@link Environment} reload, and
 * GMS startup (one environment) loads exactly once.
 */
public final class RateLimitEffectiveConfig {

  /**
   * Canonical (kebab) bind name. Binder rejects uppercase; relaxed binding still matches {@code
   * datahub.gms.rateLimits.*}.
   */
  public static final String BIND_PREFIX = "datahub.gms.rate-limits";

  private static final Object LOCK = new Object();
  private static volatile Environment loadedFrom;
  private static volatile RateLimitProperties cached;

  private RateLimitEffectiveConfig() {}

  public static RateLimitProperties get(Environment environment) {
    return get(environment, null);
  }

  /**
   * @param fromSpring Spring-bound {@code datahub.gms.rateLimits} when the {@link
   *     com.linkedin.metadata.config.GMSConfiguration} bean already exists; ignored when this
   *     environment was already loaded (Hazelcast conditions typically bind first)
   */
  public static RateLimitProperties get(Environment environment, RateLimitProperties fromSpring) {
    RateLimitProperties existing = cached;
    if (existing != null && loadedFrom == environment) {
      return existing;
    }
    synchronized (LOCK) {
      if (cached != null && loadedFrom == environment) {
        return cached;
      }
      RateLimitProperties base = fromSpring != null ? fromSpring : bind(environment);
      RateLimitConfigLoader loader =
          new RateLimitConfigLoader(new ObjectMapper(), new YAMLMapper());
      cached = loader.loadEffective(base, environment);
      loadedFrom = environment;
      return cached;
    }
  }

  static RateLimitProperties bind(Environment environment) {
    return Binder.get(environment)
        .bind(BIND_PREFIX, RateLimitProperties.class)
        .orElseGet(RateLimitProperties::new);
  }

  /** Visible for tests that share an {@link Environment} across cases. */
  public static void reset() {
    synchronized (LOCK) {
      cached = null;
      loadedFrom = null;
    }
  }
}
