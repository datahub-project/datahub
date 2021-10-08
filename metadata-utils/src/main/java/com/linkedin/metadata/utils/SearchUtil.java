package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.FilterValue;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SearchUtil {
  private static final String URN_PREFIX = "urn:";

  private SearchUtil() {
  }

  public static List<FilterValue> convertToFilters(Map<String, Long> aggregations) {
    return aggregations.entrySet().stream().map(entry -> {
      FilterValue value = new FilterValue().setValue(entry.getKey()).setFacetCount(entry.getValue());
      if (entry.getKey().startsWith(URN_PREFIX)) {
        try {
          value.setEntity(Urn.createFromString(entry.getKey()));
        } catch (URISyntaxException e) {
          log.error("Failed to create urn for filter value: {}", entry.getKey());
        }
      }
      return value;
    }).sorted(Comparator.comparingLong(value -> -value.getFacetCount())).collect(Collectors.toList());
  }
}
