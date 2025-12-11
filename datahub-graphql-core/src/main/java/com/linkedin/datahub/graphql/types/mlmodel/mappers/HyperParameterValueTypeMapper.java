/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BooleanBox;
import com.linkedin.datahub.graphql.generated.FloatBox;
import com.linkedin.datahub.graphql.generated.HyperParameterValueType;
import com.linkedin.datahub.graphql.generated.IntBox;
import com.linkedin.datahub.graphql.generated.StringBox;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class HyperParameterValueTypeMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.HyperParameterValueType, HyperParameterValueType> {

  public static final HyperParameterValueTypeMapper INSTANCE = new HyperParameterValueTypeMapper();

  public static HyperParameterValueType map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.HyperParameterValueType input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public HyperParameterValueType apply(
      @Nullable QueryContext context,
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
      result = new FloatBox(Double.valueOf(input.getFloat()));
    } else {
      throw new RuntimeException("Type is not one of the Union Types, Type: " + input.toString());
    }
    return result;
  }
}
