package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataTransform;
import com.linkedin.datahub.graphql.generated.DataTransformLogic;
import com.linkedin.datahub.graphql.generated.QueryLanguage;
import com.linkedin.datahub.graphql.generated.QueryStatement;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataTransformLogicMapper
    implements ModelMapper<
        com.linkedin.common.DataTransformLogic,
        com.linkedin.datahub.graphql.generated.DataTransformLogic> {

  public static final DataTransformLogicMapper INSTANCE = new DataTransformLogicMapper();

  public static DataTransformLogic map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransformLogic input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public DataTransformLogic apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransformLogic input) {

    final DataTransformLogic result = new DataTransformLogic();

    // Map transforms array using DataTransformMapper
    result.setTransforms(
        input.getTransforms().stream()
            .map(transform -> DataTransformMapper.map(context, transform))
            .collect(Collectors.toList()));

    return result;
  }
}

class DataTransformMapper
    implements ModelMapper<
        com.linkedin.common.DataTransform, com.linkedin.datahub.graphql.generated.DataTransform> {

  public static final DataTransformMapper INSTANCE = new DataTransformMapper();

  public static DataTransform map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransform input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public DataTransform apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.common.DataTransform input) {

    final DataTransform result = new DataTransform();

    // Map query statement if present
    if (input.hasQueryStatement()) {
      QueryStatement statement =
          new QueryStatement(
              input.getQueryStatement().getValue(),
              QueryLanguage.valueOf(input.getQueryStatement().getLanguage().toString()));
      result.setQueryStatement(statement);
    }

    return result;
  }
}
