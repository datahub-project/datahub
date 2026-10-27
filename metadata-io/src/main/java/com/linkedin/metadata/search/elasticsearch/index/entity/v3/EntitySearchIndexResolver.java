package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared V2 vs V3 entity index list for non-vector (keyword) reads: search, list, browse,
 * aggregations, analytics, and telemetry counts. V3 reads when {@code keywordReadEnabled} is true,
 * or when V3 is on and V2 is off.
 */
public final class EntitySearchIndexResolver {

  private EntitySearchIndexResolver() {}

  public static boolean shouldReadV3(@Nullable EntityIndexConfiguration entityIndex) {
    if (entityIndex == null || entityIndex.getV3() == null || !entityIndex.getV3().isEnabled()) {
      return false;
    }
    if (entityIndex.getV3().isKeywordReadEnabled()) {
      return true;
    }
    return entityIndex.getV2() == null || !entityIndex.getV2().isEnabled();
  }

  @Nonnull
  public static String[] indexNames(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<String> entityNames,
      @Nullable EntityIndexConfiguration entityIndex) {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    if (!shouldReadV3(entityIndex)) {
      return entityNames.stream()
          .map(name -> convention.getEntityIndexName(opContext, name))
          .distinct()
          .toArray(String[]::new);
    }
    Set<String> keys = new LinkedHashSet<>();
    for (String entityName : entityNames) {
      EntitySpec spec = opContext.getEntityRegistry().getEntitySpec(entityName);
      keys.add(V3IndexKeys.resolve(spec));
    }
    return keys.stream()
        .map(key -> convention.getEntityIndexNameV3(opContext, key))
        .toArray(String[]::new);
  }

  @Nonnull
  public static String indexName(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nullable EntityIndexConfiguration entityIndex) {
    String[] names = indexNames(opContext, List.of(entityName), entityIndex);
    return names[0];
  }

  /**
   * Cross-entity wildcard for reads that scan every entity index at once: analytics charts and
   * aggregations without an entity list. V2 is {@code *index_v2}; V3 is {@code *index_v3}. Only the
   * family being read is matched, so entities written to both V2 and V3 are counted once.
   */
  @Nonnull
  public static String allEntityIndexPattern(
      @Nonnull OperationContext opContext, @Nullable EntityIndexConfiguration entityIndex) {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    if (shouldReadV3(entityIndex)) {
      return convention.getV3EntityIndexPatterns(opContext).get(0);
    }
    return convention.getEntityIndexName(opContext, "*");
  }
}
