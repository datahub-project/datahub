package com.linkedin.metadata.utils;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.common.urn.Urn;
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

  private static Criterion transformEntityTypeCriterion(
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

  private static ConjunctiveCriterion transformConjunctiveCriterion(
      ConjunctiveCriterion conjunctiveCriterion, IndexConvention indexConvention) {
    return new ConjunctiveCriterion()
        .setAnd(
            conjunctiveCriterion.getAnd().stream()
                .map(
                    criterion ->
                        criterion.getField().equalsIgnoreCase(INDEX_VIRTUAL_FIELD)
                            ? transformEntityTypeCriterion(criterion, indexConvention)
                            : criterion)
                .collect(Collectors.toCollection(CriterionArray::new)));
  }

  private static ConjunctiveCriterionArray transformConjunctiveCriterionArray(
      ConjunctiveCriterionArray criterionArray, IndexConvention indexConvention) {
    return new ConjunctiveCriterionArray(
        criterionArray.stream()
            .map(
                conjunctiveCriterion ->
                    transformConjunctiveCriterion(conjunctiveCriterion, indexConvention))
            .collect(Collectors.toList()));
  }

  /**
   * Allows filtering on entities which are stored as different indices under the hood by
   * transforming the tag _entityType to _index and updating the type to the index name.
   *
   * @param filter The filter to parse and transform if needed
   * @param indexConvention The index convention used to generate the index name for an entity
   * @return A filter, with the changes if necessary
   */
  public static Filter transformFilterForEntities(
      Filter filter, @Nonnull IndexConvention indexConvention) {
    if (filter != null && filter.getOr() != null) {
      return new Filter()
          .setOr(transformConjunctiveCriterionArray(filter.getOr(), indexConvention));
    }
    return filter;
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
