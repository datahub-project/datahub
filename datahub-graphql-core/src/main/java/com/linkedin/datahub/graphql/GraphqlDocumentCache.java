package com.linkedin.datahub.graphql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.language.Document;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Cache of parsed and schema-validated GraphQL documents, populated only during query execution
 * ({@link GraphQLEngine}, via {@link GraphqlPreparsedDocumentProvider}). {@code
 * GraphqlDocumentAnalyzer} (rate-limit path) only reads from it, through {@link
 * GraphQLEngine#getCachedDocument}, so a query is parsed and validated at most once even though
 * both paths need its AST. Follows the pattern recommended in <a
 * href="https://graphql-java.com/documentation/execution/#query-caching">...</a>
 *
 * <p>Keys are a SHA-256 digest of the query text rather than the text itself to conserve some
 * space. Weight approximates each entry's heap footprint as {@code WEIGHT_MULTIPLIER}x the query's
 * UTF-8 byte length, since the parsed AST and validation result run several times larger per node
 * than the source text. Eviction is bounded by that weight and uses Caffeine's default
 * Window-TinyLFU policy, which approximates least-frequently-used eviction.
 *
 * <p>Entries expire after an hour of no access so ad-hoc queries do not occupy a cache slot.
 *
 * <p>One cache instance is meant to live for the lifetime of one {@link GraphQLEngine} (and its
 * schema); There is no invalidation.
 */
final class GraphqlDocumentCache {

  static final long DEFAULT_MAXIMUM_WEIGHT_BYTES = 25L * 1024 * 1024;
  private static final int WEIGHT_MULTIPLIER = 5;

  private record Entry(PreparsedDocumentEntry document, int weight) {}

  private final Cache<String, Entry> cache;
  private volatile boolean enabled = true;

  GraphqlDocumentCache(long maximumWeightBytes) {
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(maximumWeightBytes)
            .weigher((String key, Entry value) -> value.weight())
            .expireAfterAccess(Duration.ofHours(1))
            .recordStats()
            .build();
  }

  void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  boolean isEnabled() {
    return enabled;
  }

  /** Adjusts the cache's maximum weight in place, without rebuilding the cache. */
  void setMaximumWeightBytes(long maximumWeightBytes) {
    cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(maximumWeightBytes));
  }

  long getMaximumWeightBytes() {
    return cache.policy().eviction().map(eviction -> eviction.getMaximum()).orElse(-1L);
  }

  @Nullable
  Duration getExpireAfterAccess() {
    return cache
        .policy()
        .expireAfterAccess()
        .map(expiration -> expiration.getExpiresAfter())
        .orElse(null);
  }

  @Nonnull
  CacheStats stats() {
    return cache.stats();
  }

  void registerMetrics(@Nonnull MeterRegistry meterRegistry) {
    registerMetrics(meterRegistry, "graphqlDocumentCache");
  }

  void registerMetrics(@Nonnull MeterRegistry meterRegistry, @Nonnull String cacheName) {
    MicrometerMetricsRegistry.registerCacheMetrics(cacheName, cache, meterRegistry);
  }

  /** Write path: used only by query execution, which needs a full parse + schema validation. */
  @Nonnull
  PreparsedDocumentEntry getOrCompute(
      @Nonnull String query, @Nonnull Supplier<PreparsedDocumentEntry> parseAndValidateFunction) {
    if (!enabled) {
      return parseAndValidateFunction.get();
    }
    byte[] utf8 = query.getBytes(StandardCharsets.UTF_8);
    return cache
        .get(digest(utf8), key -> new Entry(parseAndValidateFunction.get(), weightOf(utf8)))
        .document();
  }

  /** Read-only: never populates the cache, so a caller can't cache an unvalidated document. */
  @Nullable
  Document getCachedDocument(@Nonnull String query) {
    if (!enabled) {
      return null;
    }
    Entry cached = cache.getIfPresent(digest(query.getBytes(StandardCharsets.UTF_8)));
    return cached != null ? cached.document().getDocument() : null;
  }

  void cleanUpForTesting() {
    cache.cleanUp();
  }

  private static int weightOf(byte[] utf8Query) {
    return utf8Query.length * WEIGHT_MULTIPLIER;
  }

  private static String digest(byte[] utf8Query) {
    try {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(utf8Query);
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
