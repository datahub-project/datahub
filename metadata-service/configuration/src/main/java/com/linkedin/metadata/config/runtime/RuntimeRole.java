package com.linkedin.metadata.config.runtime;

import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.core.env.Environment;

/**
 * Which process this JVM is. {@code service} hosts the service caches. {@code client} (standalone
 * MCL/MCP) and {@code upgrade} do not: both force the entity graph cache, search cache, and
 * rate-limit layers off. SQL coordination ({@code RETENTION_BUFFER_ENABLED}, {@code
 * ENTITY_WRITE_LOCK_BACKEND}) stays operator-controlled on every role, because standalone MCP
 * writes through Ebean. {@code upgrade} currently uses the same service-cache policy as {@code
 * client}.
 */
public enum RuntimeRole {
  SERVICE("service"),
  CLIENT("client"),
  UPGRADE("upgrade");

  public static final String PROPERTY = "datahub.runtime.role";

  /**
   * Bound properties forced off when {@link #disablesDistributedCaches()} is true. Keys are the
   * canonical Spring names (not the env-var aliases), so the overlay beats a Helm value after
   * placeholder resolution.
   */
  public static final List<ForcedProperty> DISTRIBUTED_CACHE_OVERRIDES =
      List.of(
          new ForcedProperty("datahub.gms.entityGraphCache.enabled", "false"),
          new ForcedProperty("searchService.cacheImplementation", "caffeine"),
          new ForcedProperty("datahub.gms.rateLimits.endpoint.enabled", "false"),
          new ForcedProperty("datahub.gms.rateLimits.scoped.enabled", "false"));

  private final String wireName;

  RuntimeRole(String wireName) {
    this.wireName = wireName;
  }

  public String wireName() {
    return wireName;
  }

  /**
   * True for {@code client} and {@code upgrade}. {@code upgrade} matches {@code client} for now.
   */
  public boolean disablesDistributedCaches() {
    return this == CLIENT || this == UPGRADE;
  }

  @Nonnull
  public static RuntimeRole from(@Nullable Environment environment) {
    String raw = environment == null ? null : environment.getProperty(PROPERTY);
    if (raw == null) {
      return SERVICE;
    }
    if (raw.isBlank()) {
      throw new IllegalStateException(
          PROPERTY + " must be service, client, or upgrade, but was blank");
    }
    String normalized = raw.trim().toLowerCase(Locale.ROOT);
    for (RuntimeRole role : values()) {
      if (role.wireName.equals(normalized)) {
        return role;
      }
    }
    throw new IllegalStateException(
        PROPERTY + " must be service, client, or upgrade, but was '" + raw.trim() + "'");
  }

  public record ForcedProperty(String key, String value) {}
}
