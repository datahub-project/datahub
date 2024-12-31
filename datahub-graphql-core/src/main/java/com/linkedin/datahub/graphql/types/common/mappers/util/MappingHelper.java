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
