package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ResultsType;
import com.linkedin.datahub.graphql.generated.StringBox;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class ResultsTypeMapper
    implements ModelMapper<com.linkedin.ml.metadata.ResultsType, ResultsType> {

  public static final ResultsTypeMapper INSTANCE = new ResultsTypeMapper();

  public static ResultsType map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.ResultsType input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public ResultsType apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.ResultsType input) {
    final ResultsType result;
    if (input.isString()) {
      result = new StringBox(input.getString());
    } else {
      throw new RuntimeException("Type is not one of the Union Types, Type:" + input.toString());
    }
    return result;
  }
}
