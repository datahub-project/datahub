package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLPrimaryKeyPropertiesMapper
    implements ModelMapper<
        com.linkedin.ml.metadata.MLPrimaryKeyProperties, MLPrimaryKeyProperties> {

  public static final MLPrimaryKeyPropertiesMapper INSTANCE = new MLPrimaryKeyPropertiesMapper();

  public static MLPrimaryKeyProperties map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties) {
    return INSTANCE.apply(context, mlPrimaryKeyProperties);
  }

  @Override
  public MLPrimaryKeyProperties apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties) {
    final MLPrimaryKeyProperties result = new MLPrimaryKeyProperties();

    result.setDescription(mlPrimaryKeyProperties.getDescription());
    if (mlPrimaryKeyProperties.getDataType() != null) {
      result.setDataType(
          MLFeatureDataType.valueOf(mlPrimaryKeyProperties.getDataType().toString()));
    }
    if (mlPrimaryKeyProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(context, mlPrimaryKeyProperties.getVersion()));
    }
    result.setSources(
        mlPrimaryKeyProperties.getSources().stream()
            .map(
                urn -> {
                  final Dataset dataset = new Dataset();
                  dataset.setUrn(urn.toString());
                  return dataset;
                })
            .collect(Collectors.toList()));

    return result;
  }
}
