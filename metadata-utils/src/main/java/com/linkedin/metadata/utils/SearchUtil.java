package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.FilterValue;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SearchUtil {
  private static final String URN_PREFIX = "urn:";

  private SearchUtil() {
  }

  /*
   * @param aggregations the aggregations coming back from elasticsearch combined with the filters from the search request
   * @param filteredValues the set of values provided by the search request
   */
  public static List<FilterValue> convertToFilters(Map<String, Long> aggregations, Set<String> filteredValues) {
    return aggregations.entrySet().stream().map(entry -> {
      return createFilterValue(entry.getKey(), entry.getValue(), filteredValues.contains(entry.getKey()));
    }).sorted(Comparator.comparingLong(value -> -value.getFacetCount())).collect(Collectors.toList());
  }

  public static FilterValue createFilterValue(String value, Long facetCount, Boolean isFilteredOn) {
    FilterValue result = new FilterValue().setValue(value).setFacetCount(facetCount).setFiltered(isFilteredOn);
    if (value.startsWith(URN_PREFIX)) {
      try {
        result.setEntity(Urn.createFromString(value));
      } catch (URISyntaxException e) {
        log.error("Failed to create urn for filter value: {}", value);
      }
    }
    return result;
  }
}
