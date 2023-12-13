package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.BooleanBox;
import com.linkedin.datahub.graphql.generated.FloatBox;
import com.linkedin.datahub.graphql.generated.HyperParameterValueType;
import com.linkedin.datahub.graphql.generated.IntBox;
import com.linkedin.datahub.graphql.generated.StringBox;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class HyperParameterValueTypeMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.HyperParameterValueType, HyperParameterValueType> {

  public static final HyperParameterValueTypeMapper INSTANCE = new HyperParameterValueTypeMapper();

  public static HyperParameterValueType map(
      @NonNull final com.linkedin.ml.metadata.HyperParameterValueType input) {
    return INSTANCE.apply(input);
  }

  @Override
  public HyperParameterValueType apply(
      @NonNull final com.linkedin.ml.metadata.HyperParameterValueType input) {
    HyperParameterValueType result = null;

    if (input.isString()) {
      result = new StringBox(input.getString());
    } else if (input.isBoolean()) {
      result = new BooleanBox(input.getBoolean());
    } else if (input.isInt()) {
      result = new IntBox(input.getInt());
    } else if (input.isDouble()) {
      result = new FloatBox(input.getDouble());
    } else if (input.isFloat()) {
      result = new FloatBox(new Double(input.getFloat()));
    } else {
      throw new RuntimeException("Type is not one of the Union Types, Type: " + input.toString());
    }
    return result;
  }
}
