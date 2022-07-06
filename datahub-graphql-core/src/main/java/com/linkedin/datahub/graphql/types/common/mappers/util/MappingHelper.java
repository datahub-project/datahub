package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.datahub.graphql.Constants.SYSTEM_METADATA_FIELD_NAME;

import com.linkedin.data.DataMap;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class MappingHelper<O> {
  @Nonnull
  private final EnvelopedAspectMap _aspectMap;
  @Getter
  @Nonnull
  private final O result;

  public void mapToResult(@Nonnull String aspectName, @Nonnull BiConsumer<O, DataMap> consumer) {
    if (_aspectMap.containsKey(aspectName)) {
      DataMap dataMap = _aspectMap.get(aspectName).getValue().data();
      if (_aspectMap.get(aspectName).hasSystemMetadata()) {
        dataMap.put(SYSTEM_METADATA_FIELD_NAME, _aspectMap.get(aspectName).getSystemMetadata().data());
      }
      consumer.accept(result, dataMap);
    }
  }
}
