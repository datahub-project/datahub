package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLFeatureProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import lombok.NonNull;

public class MLFeaturePropertiesMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLFeatureProperties, MLFeatureProperties> {

  public static final MLFeaturePropertiesMapper INSTANCE = new MLFeaturePropertiesMapper();

  public static MLFeatureProperties map(
      @NonNull final com.linkedin.ml.metadata.MLFeatureProperties mlFeatureProperties) {
    return INSTANCE.apply(mlFeatureProperties);
  }

  @Override
  public MLFeatureProperties apply(
      @NonNull final com.linkedin.ml.metadata.MLFeatureProperties mlFeatureProperties) {
    final MLFeatureProperties result = new MLFeatureProperties();

    result.setDescription(mlFeatureProperties.getDescription());
    if (mlFeatureProperties.getDataType() != null) {
      result.setDataType(MLFeatureDataType.valueOf(mlFeatureProperties.getDataType().toString()));
    }
    if (mlFeatureProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(mlFeatureProperties.getVersion()));
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

    return result;
  }
}
