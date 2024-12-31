package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SubTypesMapper
    implements ModelMapper<SubTypes, com.linkedin.datahub.graphql.generated.SubTypes> {

  public static final SubTypesMapper INSTANCE = new SubTypesMapper();

  public static com.linkedin.datahub.graphql.generated.SubTypes map(
      @Nullable QueryContext context, @Nonnull final SubTypes metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.SubTypes apply(
      @Nullable QueryContext context, @Nonnull final SubTypes input) {
    final com.linkedin.datahub.graphql.generated.SubTypes result =
        new com.linkedin.datahub.graphql.generated.SubTypes();
    result.setTypeNames(new ArrayList<>(input.getTypeNames()));
    return result;
  }
}
