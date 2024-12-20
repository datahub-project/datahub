package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLPrimaryKeyPropertiesMapper
    implements EmbeddedModelMapper<
        com.linkedin.ml.metadata.MLPrimaryKeyProperties, MLPrimaryKeyProperties> {

  public static final MLPrimaryKeyPropertiesMapper INSTANCE = new MLPrimaryKeyPropertiesMapper();

  public static MLPrimaryKeyProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, mlPrimaryKeyProperties, entityUrn);
  }

  @Override
  public MLPrimaryKeyProperties apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties,
      @Nonnull Urn entityUrn) {
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

    result.setCustomProperties(
        CustomPropertiesMapper.map(mlPrimaryKeyProperties.getCustomProperties(), entityUrn));

    return result;
  }
}
