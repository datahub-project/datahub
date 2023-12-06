package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

@Slf4j
public class SearchUtil {
  public static final String AGGREGATION_SEPARATOR_CHAR = "␞";
  public static final String INDEX_VIRTUAL_FIELD = "_entityType";
  public static final String KEYWORD_SUFFIX = ".keyword";
  private static final String URN_PREFIX = "urn:";
  private static final String REMOVED = "removed";

  private SearchUtil() {}

  /*
   * @param aggregations the aggregations coming back from elasticsearch combined with the filters from the search request
   * @param filteredValues the set of values provided by the search request
   */
  public static List<FilterValue> convertToFilters(
      Map<String, Long> aggregations, Set<String> filteredValues) {
    return aggregations.entrySet().stream()
        .map(
            entry -> {
              return createFilterValue(
                  entry.getKey(), entry.getValue(), filteredValues.contains(entry.getKey()));
            })
        .sorted(Comparator.comparingLong(value -> -value.getFacetCount()))
        .collect(Collectors.toList());
  }

  public static FilterValue createFilterValue(String value, Long facetCount, Boolean isFilteredOn) {
    // TODO(indy): test this
    String[] aggregationTokens = value.split(AGGREGATION_SEPARATOR_CHAR);
    FilterValue result =
        new FilterValue().setValue(value).setFacetCount(facetCount).setFiltered(isFilteredOn);
    String lastValue = aggregationTokens[aggregationTokens.length - 1];
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
    return criterion
        .setField("_index")
        .setValues(
            new StringArray(
                criterion.getValues().stream()
                    .map(value -> String.join("", value.split("_")))
                    .map(indexConvention::getEntityIndexName)
                    .collect(Collectors.toList())))
        .setValue(
            indexConvention.getEntityIndexName(String.join("", criterion.getValue().split("_"))));
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

  /**
   * Applies a default filter to remove entities that are soft deleted only if there isn't a filter
   * for the REMOVED field already
   */
  public static BoolQueryBuilder filterSoftDeletedByDefault(
      @Nullable Filter filter, @Nullable BoolQueryBuilder filterQuery) {
    boolean removedInOrFilter = false;
    if (filter != null) {
      removedInOrFilter =
          filter.getOr().stream()
              .anyMatch(
                  or ->
                      or.getAnd().stream()
                          .anyMatch(
                              criterion ->
                                  criterion.getField().equals(REMOVED)
                                      || criterion.getField().equals(REMOVED + KEYWORD_SUFFIX)));
    }
    if (!removedInOrFilter) {
      filterQuery.mustNot(QueryBuilders.matchQuery(REMOVED, true));
    }
    return filterQuery;
  }
}
