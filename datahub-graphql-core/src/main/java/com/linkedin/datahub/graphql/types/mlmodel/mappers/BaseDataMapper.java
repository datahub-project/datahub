package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BaseData;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class BaseDataMapper implements ModelMapper<com.linkedin.ml.metadata.BaseData, BaseData> {
  public static final BaseDataMapper INSTANCE = new BaseDataMapper();

  public static BaseData map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.BaseData input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public BaseData apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.BaseData input) {
    final BaseData result = new BaseData();
    result.setDataset(input.getDataset().toString());
    result.setMotivation(input.getMotivation());
    result.setPreProcessing(input.getPreProcessing());
    return result;
  }
}
