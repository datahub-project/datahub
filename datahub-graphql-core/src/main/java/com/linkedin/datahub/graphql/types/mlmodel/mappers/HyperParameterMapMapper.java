package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.HyperParameterMap;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.ml.metadata.HyperParameterValueTypeMap;
import javax.annotation.Nullable;
import lombok.NonNull;

public class HyperParameterMapMapper
    implements ModelMapper<HyperParameterValueTypeMap, HyperParameterMap> {

  public static final HyperParameterMapMapper INSTANCE = new HyperParameterMapMapper();

  public static HyperParameterMap map(
      @Nullable QueryContext context, @NonNull final HyperParameterValueTypeMap input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public HyperParameterMap apply(
      @Nullable QueryContext context, @NonNull final HyperParameterValueTypeMap input) {
    final HyperParameterMap result = new HyperParameterMap();

    for (String key : input.keySet()) {
      result.setKey(key);
      result.setValue(HyperParameterValueTypeMapper.map(context, input.get(key)));
    }

    return result;
  }
}
