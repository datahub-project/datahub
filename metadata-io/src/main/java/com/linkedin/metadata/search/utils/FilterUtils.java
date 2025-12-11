/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.utils;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.AggregationMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterUtils {

  private FilterUtils() {}

  private static final List<String> FILTER_RANKING =
      ImmutableList.of(
          "_entityType",
          "typeNames",
          "platform",
          "domains",
          "tags",
          "glossaryTerms",
          "container",
          "owners",
          "origin");

  public static List<AggregationMetadata> rankFilterGroups(
      Map<String, AggregationMetadata> aggregations) {
    Set<String> filterGroups = new HashSet<>(aggregations.keySet());
    List<AggregationMetadata> finalAggregations = new ArrayList<>(aggregations.size());
    for (String filterName : FILTER_RANKING) {
      if (filterGroups.contains(filterName)) {
        filterGroups.remove(filterName);
        finalAggregations.add(aggregations.get(filterName));
      }
    }
    filterGroups.forEach(filterName -> finalAggregations.add(aggregations.get(filterName)));
    return finalAggregations;
  }
}
