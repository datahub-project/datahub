package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.data.DataMap;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
}
