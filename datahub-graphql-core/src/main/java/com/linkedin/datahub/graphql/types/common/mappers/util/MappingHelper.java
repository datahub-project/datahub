/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.function.TriConsumer;

@AllArgsConstructor
public class MappingHelper<O> {
  @Nonnull private final EnvelopedAspectMap _aspectMap;
  @Getter @Nonnull private final O result;

  public void mapToResult(@Nonnull String aspectName, @Nonnull BiConsumer<O, DataMap> consumer) {
    if (_aspectMap.containsKey(aspectName)) {
      DataMap dataMap = _aspectMap.get(aspectName).getValue().data();
      consumer.accept(result, dataMap);
    }
  }

  public void mapToResult(
      @Nullable QueryContext context,
      @Nonnull String aspectName,
      @Nonnull TriConsumer<QueryContext, O, DataMap> consumer) {
    if (_aspectMap.containsKey(aspectName)) {
      DataMap dataMap = _aspectMap.get(aspectName).getValue().data();
      consumer.accept(context, result, dataMap);
    }
  }
}
