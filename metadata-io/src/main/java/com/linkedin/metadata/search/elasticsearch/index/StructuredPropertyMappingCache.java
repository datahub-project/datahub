package com.linkedin.metadata.search.elasticsearch.index;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Field names under {@code structuredProperties.properties} for one search index, cached so a
 * restate of many rows reads each index's mapping once per TTL.
 *
 * <p>A failed read is not cached. The caller writes the document it already has and leaves stale
 * fields until the next restate that can read the mapping. The aspect itself is already stored.
 */
@Slf4j
public class StructuredPropertyMappingCache {

  private static final String PROPERTIES = "properties";

  private final boolean enabled;
  private final Cache<String, Set<String>> cache;
  private final MappingSource source;

  /** Reads the mapping body of one index. An index with no mapping returns an empty body. */
  @FunctionalInterface
  public interface MappingSource {
    @Nonnull
    Map<String, Object> mapping(@Nonnull OperationContext opContext, @Nonnull String indexName)
        throws IOException;
  }

  public StructuredPropertyMappingCache(
      boolean enabled, long ttlSeconds, @Nonnull MappingSource source) {
    this(enabled, ttlSeconds, Ticker.systemTicker(), source);
  }

  @VisibleForTesting
  StructuredPropertyMappingCache(
      boolean enabled, long ttlSeconds, @Nonnull Ticker ticker, @Nonnull MappingSource source) {
    this.enabled = enabled;
    this.source = source;
    this.cache =
        CacheBuilder.newBuilder()
            .ticker(ticker)
            .expireAfterWrite(Math.max(ttlSeconds, 0), TimeUnit.SECONDS)
            .maximumSize(500)
            .build();
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Dotted {@code structuredProperties.<child>} names mapped on {@code indexName}. Empty when the
   * feature is off or the index maps no structured properties. {@link Optional#empty()} when the
   * mapping could not be read; that result is not cached.
   */
  @Nonnull
  public Optional<Set<String>> fields(
      @Nonnull OperationContext opContext, @Nullable String indexName) {
    if (!enabled || indexName == null || indexName.isEmpty()) {
      return Optional.of(Set.of());
    }
    Set<String> cached = cache.getIfPresent(indexName);
    if (cached != null) {
      return Optional.of(cached);
    }
    try {
      Set<String> loaded = fieldNames(source.mapping(opContext, indexName));
      cache.put(indexName, loaded);
      return Optional.of(loaded);
    } catch (Exception e) {
      log.warn(
          "Failed to read structured property mappings for index {}; writing without catalog nulls",
          indexName,
          e);
      return Optional.empty();
    }
  }

  /**
   * Dotted document keys under {@code structuredProperties}, including a V3 {@code __urn_name}
   * sibling. An unwrapped mapping body and a legacy type-wrapped body are both accepted.
   *
   * <p>Unversioned properties are first-level children. Versioned properties are a nested {@code
   * _versioned} object in the mapping and dotted leaves on the document ({@code
   * structuredProperties._versioned.<name>.<version>.<type>}). The parent object is not a field:
   * nulling it would clear leaves the aspect still sets, and would leave removed leaves in the
   * index. Multi-fields under {@code fields} are not separate document keys.
   */
  @Nonnull
  @VisibleForTesting
  static Set<String> fieldNames(@Nullable Map<String, Object> mapping) {
    if (mapping == null || mapping.isEmpty()) {
      return Set.of();
    }
    Map<String, Object> properties = childMap(mapping, PROPERTIES);
    if (properties.isEmpty()) {
      for (Object value : mapping.values()) {
        if (value instanceof Map) {
          Map<String, Object> nested = childMap(castMap(value), PROPERTIES);
          if (!nested.isEmpty()) {
            properties = nested;
            break;
          }
        }
      }
    }
    Map<String, Object> children =
        childMap(childMap(properties, STRUCTURED_PROPERTY_MAPPING_FIELD), PROPERTIES);
    if (children.isEmpty()) {
      return Set.of();
    }
    Set<String> names = new LinkedHashSet<>();
    for (Map.Entry<String, Object> entry : children.entrySet()) {
      String child = entry.getKey();
      if (child == null || child.isEmpty()) {
        continue;
      }
      String path = STRUCTURED_PROPERTY_MAPPING_FIELD + "." + child;
      if (STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD.equals(child)
          && entry.getValue() instanceof Map) {
        collectVersionedLeaves(path, castMap(entry.getValue()), names);
      } else {
        names.add(path);
      }
    }
    return Set.copyOf(names);
  }

  /**
   * Walks the nested {@code _versioned} object down to field mappings. A leaf declares a scalar
   * {@code type} and has no nested {@code properties}.
   */
  private static void collectVersionedLeaves(
      @Nonnull String prefix, @Nonnull Map<String, Object> node, @Nonnull Set<String> names) {
    Map<String, Object> properties = childMap(node, PROPERTIES);
    // GetMappings reports an object as type "object" plus properties. That node is not a leaf.
    if (!properties.isEmpty()) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        String child = entry.getKey();
        if (child == null || child.isEmpty() || !(entry.getValue() instanceof Map)) {
          continue;
        }
        collectVersionedLeaves(prefix + "." + child, castMap(entry.getValue()), names);
      }
      return;
    }
    if (hasScalarType(node)) {
      names.add(prefix);
    }
  }

  private static boolean hasScalarType(@Nonnull Map<String, Object> node) {
    Object type = node.get("type");
    return type != null && !(type instanceof Map);
  }

  @Nonnull
  private static Map<String, Object> childMap(
      @Nonnull Map<String, Object> parent, @Nonnull String key) {
    Object value = parent.get(key);
    return value instanceof Map ? castMap(value) : Map.of();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(@Nonnull Object value) {
    return (Map<String, Object>) value;
  }
}
