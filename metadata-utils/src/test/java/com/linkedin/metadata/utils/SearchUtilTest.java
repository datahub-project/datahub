/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.utils;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.FilterValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntityKeyUtils} */
public class SearchUtilTest {

  @Test
  public void testConvertToFilters() throws Exception {
    Map<String, Long> aggregations = new HashMap<>();
    aggregations.put("urn:li:tag:abc", 3L);
    aggregations.put("urn:li:tag:def", 0L);

    Set<String> filteredValues = ImmutableSet.of("urn:li:tag:def");

    List<FilterValue> filters = SearchUtil.convertToFilters(aggregations, filteredValues);

    assertEquals(
        filters.get(0),
        new FilterValue()
            .setFiltered(false)
            .setValue("urn:li:tag:abc")
            .setEntity(Urn.createFromString("urn:li:tag:abc"))
            .setFacetCount(3L));

    assertEquals(
        filters.get(1),
        new FilterValue()
            .setFiltered(true)
            .setValue("urn:li:tag:def")
            .setEntity(Urn.createFromString("urn:li:tag:def"))
            .setFacetCount(0L));
  }
}
