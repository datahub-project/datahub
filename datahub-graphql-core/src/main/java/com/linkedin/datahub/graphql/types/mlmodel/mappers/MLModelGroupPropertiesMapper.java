package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLModelGroupProperties;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLModelGroupPropertiesMapper
    implements EmbeddedModelMapper<
        com.linkedin.ml.metadata.MLModelGroupProperties, MLModelGroupProperties> {

  public static final MLModelGroupPropertiesMapper INSTANCE = new MLModelGroupPropertiesMapper();

  public static MLModelGroupProperties map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, mlModelGroupProperties, entityUrn);
  }

  @Override
  public MLModelGroupProperties apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLModelGroupProperties mlModelGroupProperties,
      @Nonnull Urn entityUrn) {
    final MLModelGroupProperties result = new MLModelGroupProperties();

    result.setDescription(mlModelGroupProperties.getDescription());
    if (mlModelGroupProperties.getVersion() != null) {
      result.setVersion(VersionTagMapper.map(context, mlModelGroupProperties.getVersion()));
    }
    result.setCreatedAt(mlModelGroupProperties.getCreatedAt());

    result.setCustomProperties(
        CustomPropertiesMapper.map(mlModelGroupProperties.getCustomProperties(), entityUrn));

    return result;
  }
}
