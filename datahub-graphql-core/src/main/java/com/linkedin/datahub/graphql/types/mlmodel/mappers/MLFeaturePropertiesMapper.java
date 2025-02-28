package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLFeatureProperties;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLFeaturePropertiesMapper
    implements EmbeddedModelMapper<
        com.linkedin.ml.metadata.MLFeatureProperties, MLFeatureProperties> {

  public static final MLFeaturePropertiesMapper INSTANCE = new MLFeaturePropertiesMapper();

  public static MLFeatureProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLFeatureProperties mlFeatureProperties,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, mlFeatureProperties, entityUrn);
  }

  @Override
  public MLFeatureProperties apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLFeatureProperties mlFeatureProperties,
      @Nonnull Urn entityUrn) {
    final MLFeatureProperties result = new MLFeatureProperties();

    result.setDescription(mlFeatureProperties.getDescription());
    if (mlFeatureProperties.getDataType() != null) {
      result.setDataType(MLFeatureDataType.valueOf(mlFeatureProperties.getDataType().toString()));
    }
    if (mlFeatureProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(context, mlFeatureProperties.getVersion()));
    }
    if (mlFeatureProperties.getSources() != null) {
      result.setSources(
          mlFeatureProperties.getSources().stream()
              .map(
                  urn -> {
                    final Dataset dataset = new Dataset();
                    dataset.setUrn(urn.toString());
                    return dataset;
                  })
              .collect(Collectors.toList()));
    }

    result.setCustomProperties(
        CustomPropertiesMapper.map(mlFeatureProperties.getCustomProperties(), entityUrn));

    return result;
  }
}
