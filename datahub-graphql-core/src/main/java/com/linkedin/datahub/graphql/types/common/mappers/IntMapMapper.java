/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IntMapEntry;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class IntMapMapper implements ModelMapper<Map<String, Integer>, List<IntMapEntry>> {

  public static final IntMapMapper INSTANCE = new IntMapMapper();

  public static List<IntMapEntry> map(
      @Nullable QueryContext context, @Nonnull final Map<String, Integer> input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public List<IntMapEntry> apply(
      @Nullable QueryContext context, @Nonnull final Map<String, Integer> input) {
    List<IntMapEntry> results = new ArrayList<>();
    for (String key : input.keySet()) {
      final IntMapEntry entry = new IntMapEntry();
      entry.setKey(key);
      entry.setValue(input.get(key));
      results.add(entry);
    }
    return results;
  }
}
