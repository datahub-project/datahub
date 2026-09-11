package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

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
   * Cross-entity wildcard used by analytics charts that scan every entity index at once. V2 is
   * {@code *index_v2}; V3 is {@code *index_v3}.
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

  /**
   * Restricts a query to the requested entity types via {@code _entityType}.
   *
   * <p>V3 documents always store {@code _entityType}, including when the index is named after the
   * entity rather than a shared search group. Entity-type scoped reads and deletes must still
   * filter on the field so a shared group index (or leftover documents) cannot leak another type.
   *
   * <p>No-op for V2: those documents do not have {@code _entityType} as a real field.
   */
  public static void applyEntityTypeFilter(
      @Nonnull BoolQueryBuilder query,
      @Nonnull Collection<String> entityNames,
      @Nullable EntityIndexConfiguration entityIndex) {
    QueryBuilder entityTypeQuery = entityTypeQuery(entityNames, entityIndex);
    if (entityTypeQuery != null) {
      query.filter(entityTypeQuery);
    }
  }

  @Nullable
  public static QueryBuilder entityTypeQuery(
      @Nonnull Collection<String> entityNames, @Nullable EntityIndexConfiguration entityIndex) {
    if (!shouldReadV3(entityIndex) || entityNames.isEmpty()) {
      return null;
    }
    return QueryBuilders.termsQuery(INDEX_VIRTUAL_FIELD, entityNames);
  }
}
