package com.linkedin.metadata.utils;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchUtil {
  public static final String AGGREGATION_SEPARATOR_CHAR = "␞";
  // Currently used to delimit missing fields, leaving naming open to extend to other use cases
  public static final String AGGREGATION_SPECIAL_TYPE_DELIMITER = "␝";
  public static final String MISSING_SPECIAL_TYPE = "missing";
  public static final String INDEX_VIRTUAL_FIELD = "_entityType";
  public static final String ES_INDEX_FIELD = "_index";
  public static final String KEYWORD_SUFFIX = ".keyword";
  private static final String URN_PREFIX = "urn:";

  private SearchUtil() {}

  /*
   * @param aggregations the aggregations coming back from elasticsearch combined with the filters from the search request
   * @param filteredValues the set of values provided by the search request
   */
  public static List<FilterValue> convertToFilters(
      Map<String, Long> aggregations, Set<String> filteredValues) {
    return aggregations.entrySet().stream()
        .map(
            entry ->
                createFilterValue(
                    entry.getKey(), entry.getValue(), filteredValues.contains(entry.getKey())))
        .sorted(Comparator.comparingLong(value -> -value.getFacetCount()))
        .collect(Collectors.toList());
  }

  public static FilterValue createFilterValue(String value, Long facetCount, Boolean isFilteredOn) {
    // TODO(indy): test this
    String[] aggregations = value.split(AGGREGATION_SEPARATOR_CHAR);
    FilterValue result =
        new FilterValue().setValue(value).setFacetCount(facetCount).setFiltered(isFilteredOn);
    String lastValue = aggregations[aggregations.length - 1];
    if (lastValue.startsWith(URN_PREFIX)) {
      try {
        result.setEntity(Urn.createFromString(lastValue));
      } catch (URISyntaxException e) {
        log.error("Failed to create urn for filter value: {}", lastValue);
      }
    }
    return result;
  }

  private static Criterion transformEntityTypeCriterionForV2(
      Criterion criterion, IndexConvention indexConvention) {
    return buildCriterion(
        ES_INDEX_FIELD,
        Condition.EQUAL,
        criterion.isNegated(),
        criterion.getValues().stream()
            .map(value -> String.join("", value.split("_")))
            .map(indexConvention::getEntityIndexName)
            .collect(Collectors.toList()));
  }

  private static Criterion transformEntityTypeCriterionForV3(
      Criterion criterion, @Nullable EntityRegistry entityRegistry) {
    // V3 documents have _entityType field with camelCase entity names (e.g. "glossaryTerm",
    // "dataFlow"). Use EntityRegistry to get the canonical name since simple string conversion
    // is unreliable (e.g., "corpuser" not "corpUser", "mlModel" not "mlmodel").
    return buildCriterion(
        "_entityType",
        Condition.EQUAL,
        criterion.isNegated(),
        criterion.getValues().stream()
            .map(
                value -> {
                  String joined = String.join("", value.split("_")).toLowerCase();
                  if (entityRegistry != null) {
                    try {
                      return entityRegistry.getEntitySpec(joined).getName();
                    } catch (IllegalArgumentException e) {
                      // Entity not found in registry, fall back to lowercase
                    }
                  }
                  return joined;
                })
            .collect(Collectors.toList()));
  }

  private static List<ConjunctiveCriterion> transformConjunctiveCriterion(
      ConjunctiveCriterion conjunctiveCriterion,
      IndexConvention indexConvention,
      @Nullable EntityRegistry entityRegistry) {
    boolean hasEntityTypeFilter =
        conjunctiveCriterion.getAnd().stream()
            .anyMatch(c -> c.getField().equalsIgnoreCase(INDEX_VIRTUAL_FIELD));

    if (!hasEntityTypeFilter) {
      // No entity type filter — return as-is
      return List.of(conjunctiveCriterion);
    }

    // Split non-entity-type criteria from entity-type criteria
    List<Criterion> nonEntityCriteria =
        conjunctiveCriterion.getAnd().stream()
            .filter(c -> !c.getField().equalsIgnoreCase(INDEX_VIRTUAL_FIELD))
            .collect(Collectors.toList());
    List<Criterion> entityCriteria =
        conjunctiveCriterion.getAnd().stream()
            .filter(c -> c.getField().equalsIgnoreCase(INDEX_VIRTUAL_FIELD))
            .collect(Collectors.toList());

    // V2 variant: _index = chartindex_v2 (works for V2 indices)
    CriterionArray v2Criteria = new CriterionArray(nonEntityCriteria);
    entityCriteria.forEach(
        c -> v2Criteria.add(transformEntityTypeCriterionForV2(c, indexConvention)));
    ConjunctiveCriterion v2Conjunction = new ConjunctiveCriterion().setAnd(v2Criteria);

    // V3 variant: _entityType = glossaryTerm (works for V3 unified index)
    CriterionArray v3Criteria = new CriterionArray(nonEntityCriteria);
    entityCriteria.forEach(
        c -> v3Criteria.add(transformEntityTypeCriterionForV3(c, entityRegistry)));
    ConjunctiveCriterion v3Conjunction = new ConjunctiveCriterion().setAnd(v3Criteria);

    return List.of(v2Conjunction, v3Conjunction);
  }

  private static ConjunctiveCriterionArray transformConjunctiveCriterionArray(
      ConjunctiveCriterionArray criterionArray,
      IndexConvention indexConvention,
      @Nullable EntityRegistry entityRegistry) {
    return new ConjunctiveCriterionArray(
        criterionArray.stream()
            .flatMap(
                conjunctiveCriterion ->
                    transformConjunctiveCriterion(
                        conjunctiveCriterion, indexConvention, entityRegistry)
                        .stream())
            .collect(Collectors.toList()));
  }

  /**
   * Allows filtering on entities which are stored as different indices under the hood by
   * transforming the tag _entityType to _index and updating the type to the index name.
   *
   * @param filter The filter to parse and transform if needed
   * @param indexConvention The index convention used to generate the index name for an entity
   * @param entityRegistry The entity registry for canonical entity name lookup (V3 camelCase)
   * @return A filter, with the changes if necessary
   */
  public static Filter transformFilterForEntities(
      Filter filter,
      @Nonnull IndexConvention indexConvention,
      @Nullable EntityRegistry entityRegistry) {
    if (filter != null && filter.getOr() != null) {
      return new Filter()
          .setOr(
              transformConjunctiveCriterionArray(filter.getOr(), indexConvention, entityRegistry));
    }
    return filter;
  }

  /** Backward-compatible overload without EntityRegistry (V3 entity type filter uses lowercase). */
  public static Filter transformFilterForEntities(
      Filter filter, @Nonnull IndexConvention indexConvention) {
    return transformFilterForEntities(filter, indexConvention, null);
  }

  public static SortCriterion sortBy(@Nonnull String field, @Nullable SortOrder direction) {
    SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField(field);
    sortCriterion.setOrder(
        com.linkedin.metadata.query.filter.SortOrder.valueOf(
            Optional.ofNullable(direction).orElse(SortOrder.ASCENDING).toString()));
    return sortCriterion;
  }

  public static Filter andFilter(Criterion... criteria) {
    Filter filter = new Filter();
    filter.setOr(andCriterion(Arrays.stream(criteria)));
    return filter;
  }

  public static ConjunctiveCriterionArray andCriterion(Stream<Criterion> criteria) {
    return new ConjunctiveCriterionArray(
        new ConjunctiveCriterion()
            .setAnd(new CriterionArray(criteria.collect(Collectors.toList()))));
  }
}
