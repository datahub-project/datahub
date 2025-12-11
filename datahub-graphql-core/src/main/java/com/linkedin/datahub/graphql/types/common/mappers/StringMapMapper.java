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
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class StringMapMapper implements ModelMapper<Map<String, String>, List<StringMapEntry>> {

  public static final StringMapMapper INSTANCE = new StringMapMapper();

  public static List<StringMapEntry> map(
      @Nullable QueryContext context, @Nonnull final Map<String, String> input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public List<StringMapEntry> apply(
      @Nullable QueryContext context, @Nonnull final Map<String, String> input) {
    List<StringMapEntry> results = new ArrayList<>();
    for (String key : input.keySet()) {
      final StringMapEntry entry = new StringMapEntry();
      entry.setKey(key);
      entry.setValue(input.get(key));
      results.add(entry);
    }
    return results;
  }
}
