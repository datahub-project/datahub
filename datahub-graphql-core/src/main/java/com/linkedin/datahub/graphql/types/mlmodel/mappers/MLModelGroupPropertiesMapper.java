package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLModelGroupProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLModelGroupPropertiesMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.MLModelGroupProperties, MLModelGroupProperties> {

  public static final MLModelGroupPropertiesMapper INSTANCE = new MLModelGroupPropertiesMapper();

  public static MLModelGroupProperties map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties) {
    return INSTANCE.apply(context, mlModelGroupProperties);
  }

  @Override
  public MLModelGroupProperties apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties) {
    final MLModelGroupProperties result = new MLModelGroupProperties();

    result.setDescription(mlModelGroupProperties.getDescription());
    if (mlModelGroupProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(context, mlModelGroupProperties.getVersion()));
    }
    result.setCreatedAt(mlModelGroupProperties.getCreatedAt());

    return result;
  }
}
