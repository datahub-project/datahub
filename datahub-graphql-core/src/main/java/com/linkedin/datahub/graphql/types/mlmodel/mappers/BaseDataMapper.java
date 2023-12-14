package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.BaseData;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class BaseDataMapper implements ModelMapper<com.linkedin.ml.metadata.BaseData, BaseData> {
  public static final BaseDataMapper INSTANCE = new BaseDataMapper();

  public static BaseData map(@NonNull final com.linkedin.ml.metadata.BaseData input) {
    return INSTANCE.apply(input);
  }

  @Override
  public BaseData apply(@NonNull final com.linkedin.ml.metadata.BaseData input) {
    final BaseData result = new BaseData();
    result.setDataset(input.getDataset().toString());
    result.setMotivation(input.getMotivation());
    result.setPreProcessing(input.getPreProcessing());
    return result;
  }
}
