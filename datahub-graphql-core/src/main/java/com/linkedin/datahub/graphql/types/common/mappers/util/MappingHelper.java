package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.data.DataMap;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class MappingHelper<O> {
  private final EnvelopedAspectMap _aspectMap;
  @Getter
  private final O result;

  public void mapToResult(String aspectName, BiConsumer<O, DataMap> consumer) {
    if (_aspectMap.containsKey(aspectName)) {
      DataMap dataMap = _aspectMap.get(aspectName).getValue().data();
      consumer.accept(result, dataMap);
    }
  }
}
