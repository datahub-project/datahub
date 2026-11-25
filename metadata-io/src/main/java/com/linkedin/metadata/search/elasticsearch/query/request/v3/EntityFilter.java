package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_SUFFIX;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/** Entity-type filters that exist so a query hits a Search V3 index. */
public final class EntityFilter {

  private EntityFilter() {}

  /**
   * Returns a copy of the filter for a Search V3 entity index.
   *
   * <p>{@code _entityType} holds the registry entity name, so values such as {@code DATA_PRODUCT}
   * are mapped onto it, as V2 does when it rewrites the filter to index names.
   */
  @Nullable
  public static Filter toEntityFilter(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    if (filter == null) {
      return null;
    }
    Filter result = new Filter();
    if (filter.getOr() != null) {
      result.setOr(
          filter.getOr().stream()
              .map(
                  and ->
                      new ConjunctiveCriterion().setAnd(toEntityCriteria(opContext, and.getAnd())))
              .collect(Collectors.toCollection(ConjunctiveCriterionArray::new)));
    }
    if (filter.getCriteria() != null) {
      result.setCriteria(toEntityCriteria(opContext, filter.getCriteria()));
    }
    return result;
  }

  /**
   * Adds a case-insensitive term filter on {@code _entityType} when one index holds several types.
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
    if (!EntitySearchIndexResolver.shouldReadV3(entityIndex) || entityNames.isEmpty()) {
      return null;
    }
    // Callers often pass entity registry keys, which are lower-cased (glossaryterm), while V3
    // stores the entity name (glossaryTerm). Entity names are unique ignoring case.
    BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch(1);
    for (String entityName : entityNames) {
      query.should(QueryBuilders.termQuery(INDEX_VIRTUAL_FIELD, entityName).caseInsensitive(true));
    }
    return query;
  }

  private static CriterionArray toEntityCriteria(
      @Nonnull OperationContext opContext, @Nonnull CriterionArray criteria) {
    return criteria.stream()
        .map(criterion -> toEntityCriterion(opContext, criterion))
        .collect(Collectors.toCollection(CriterionArray::new));
  }

  private static Criterion toEntityCriterion(
      @Nonnull OperationContext opContext, @Nonnull Criterion criterion) {
    if (!StringUtils.removeEnd(criterion.getField(), KEYWORD_SUFFIX)
        .equalsIgnoreCase(INDEX_VIRTUAL_FIELD)) {
      return criterion;
    }
    // Copy rather than rebuild: a criterion without values must stay without values
    Criterion result =
        new Criterion()
            .setField(INDEX_VIRTUAL_FIELD)
            .setCondition(criterion.getCondition())
            .setNegated(criterion.isNegated());
    if (criterion.hasValues()) {
      result.setValues(
          criterion.getValues().stream()
              .map(value -> entityTypeValue(opContext, value))
              .collect(Collectors.toCollection(StringArray::new)));
    }
    return result;
  }

  private static String entityTypeValue(
      @Nonnull OperationContext opContext, @Nonnull String value) {
    EntitySpec entitySpec;
    try {
      entitySpec = opContext.getEntityRegistry().getEntitySpec(value.replace("_", ""));
    } catch (IllegalArgumentException e) {
      entitySpec = null;
    }
    // Unknown entity type: keep the value so the filter matches nothing, as on V2
    return entitySpec != null ? entitySpec.getName() : value;
  }
}
