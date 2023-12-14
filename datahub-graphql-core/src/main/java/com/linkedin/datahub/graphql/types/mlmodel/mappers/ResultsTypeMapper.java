package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.ResultsType;
import com.linkedin.datahub.graphql.generated.StringBox;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class ResultsTypeMapper
    implements ModelMapper<com.linkedin.ml.metadata.ResultsType, ResultsType> {

  public static final ResultsTypeMapper INSTANCE = new ResultsTypeMapper();

  public static ResultsType map(@NonNull final com.linkedin.ml.metadata.ResultsType input) {
    return INSTANCE.apply(input);
  }

  @Override
  public ResultsType apply(@NonNull final com.linkedin.ml.metadata.ResultsType input) {
    final ResultsType result;
    if (input.isString()) {
      result = new StringBox(input.getString());
    } else {
      throw new RuntimeException("Type is not one of the Union Types, Type:" + input.toString());
    }
    return result;
  }
}
