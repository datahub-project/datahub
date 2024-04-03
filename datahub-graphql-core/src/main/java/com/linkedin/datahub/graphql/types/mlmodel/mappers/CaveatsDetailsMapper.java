package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CaveatDetails;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class CaveatsDetailsMapper
    implements ModelMapper<com.linkedin.ml.metadata.CaveatDetails, CaveatDetails> {

  public static final CaveatsDetailsMapper INSTANCE = new CaveatsDetailsMapper();

  public static CaveatDetails map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.CaveatDetails input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public CaveatDetails apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.CaveatDetails input) {
    final CaveatDetails result = new CaveatDetails();

    result.setCaveatDescription(input.getCaveatDescription());
    result.setGroupsNotRepresented(input.getGroupsNotRepresented());
    result.setNeedsFurtherTesting(input.isNeedsFurtherTesting());
    return result;
  }
}
