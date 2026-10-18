package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;

/**
 * Resolves the V3 Elasticsearch index key for an entity: explicit {@code searchGroup} consolidates;
 * unset group uses the entity type name (not a literal {@code default} group).
 *
 * <p>{@link #groupEntitySpecs} is a snapshot of the registry at first use. A later plugin overlay
 * on the same instance is not re-read: plugins may only add new entity types (not change {@code
 * searchGroup} on types already loaded), and those new types do not get search indices today, so
 * search-group lookup does not refresh for them.
 */
public final class V3IndexKeys {

  private static final Map<EntityRegistry, Map<String, List<EntitySpec>>> GROUPED_SPECS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private V3IndexKeys() {}

  @Nonnull
  public static String resolve(@Nonnull EntitySpec spec) {
    if (!EntityAnnotation.isSearchGroupUnset(spec.getSearchGroup())) {
      return spec.getSearchGroup();
    }
    return spec.getName();
  }

  @Nonnull
  public static Map<String, List<EntitySpec>> groupEntitySpecs(
      @Nonnull EntityRegistry entityRegistry) {
    return GROUPED_SPECS.computeIfAbsent(entityRegistry, V3IndexKeys::buildGroupedSpecs);
  }

  /**
   * The entity types whose documents live in the V3 index for {@code indexKey}. Matching ignores
   * case: the index convention lower-cases keys when it forms index names, so a key read back from
   * an index name (for example {@code schemafield} from {@code schemafieldindex_v3}) must still
   * find a group declared as {@code schemaField}.
   */
  @Nonnull
  public static Collection<EntitySpec> entitySpecsForKey(
      @Nonnull EntityRegistry entityRegistry, @Nonnull String indexKey) {
    Map<String, List<EntitySpec>> grouped = groupEntitySpecs(entityRegistry);
    List<EntitySpec> specs = grouped.get(indexKey);
    if (specs != null) {
      return specs;
    }
    return grouped.entrySet().stream()
        .filter(entry -> entry.getKey().equalsIgnoreCase(indexKey))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(List.of());
  }

  @Nonnull
  private static Map<String, List<EntitySpec>> buildGroupedSpecs(
      @Nonnull EntityRegistry entityRegistry) {
    Map<String, List<EntitySpec>> byKey = new LinkedHashMap<>();
    for (EntitySpec spec : entityRegistry.getEntitySpecs().values()) {
      byKey.computeIfAbsent(resolve(spec), k -> new ArrayList<>()).add(spec);
    }
    byKey.replaceAll((key, specs) -> Collections.unmodifiableList(specs));
    return Collections.unmodifiableMap(byKey);
  }
}
