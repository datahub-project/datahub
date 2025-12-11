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
