package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.HyperParameterMap;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.ml.metadata.HyperParameterValueTypeMap;
import lombok.NonNull;

public class HyperParameterMapMapper
    implements ModelMapper<HyperParameterValueTypeMap, HyperParameterMap> {

  public static final HyperParameterMapMapper INSTANCE = new HyperParameterMapMapper();

  public static HyperParameterMap map(@NonNull final HyperParameterValueTypeMap input) {
    return INSTANCE.apply(input);
  }

  @Override
  public HyperParameterMap apply(@NonNull final HyperParameterValueTypeMap input) {
    final HyperParameterMap result = new HyperParameterMap();

    for (String key : input.keySet()) {
      result.setKey(key);
      result.setValue(HyperParameterValueTypeMapper.map(input.get(key)));
    }

    return result;
  }
}
