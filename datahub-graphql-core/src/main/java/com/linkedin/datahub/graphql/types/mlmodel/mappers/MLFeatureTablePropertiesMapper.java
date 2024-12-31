package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTableProperties;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.mappers.EmbeddedModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLFeatureTablePropertiesMapper
    implements EmbeddedModelMapper<
        com.linkedin.ml.metadata.MLFeatureTableProperties, MLFeatureTableProperties> {

  public static final MLFeatureTablePropertiesMapper INSTANCE =
      new MLFeatureTablePropertiesMapper();

  public static MLFeatureTableProperties map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLFeatureTableProperties mlFeatureTableProperties,
      @Nonnull Urn entityUrn) {
    return INSTANCE.apply(context, mlFeatureTableProperties, entityUrn);
  }

  @Override
  public MLFeatureTableProperties apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.ml.metadata.MLFeatureTableProperties mlFeatureTableProperties,
      @Nonnull Urn entityUrn) {
    final MLFeatureTableProperties result = new MLFeatureTableProperties();

    result.setDescription(mlFeatureTableProperties.getDescription());
    if (mlFeatureTableProperties.getMlFeatures() != null) {
      result.setMlFeatures(
          mlFeatureTableProperties.getMlFeatures().stream()
              .filter(f -> context == null || canView(context.getOperationContext(), f))
              .map(
                  urn -> {
                    final MLFeature mlFeature = new MLFeature();
                    mlFeature.setUrn(urn.toString());
                    return mlFeature;
                  })
              .collect(Collectors.toList()));
    }

    if (mlFeatureTableProperties.getMlPrimaryKeys() != null) {
      result.setMlPrimaryKeys(
          mlFeatureTableProperties.getMlPrimaryKeys().stream()
              .filter(k -> context == null || canView(context.getOperationContext(), k))
              .map(
                  urn -> {
                    final MLPrimaryKey mlPrimaryKey = new MLPrimaryKey();
                    mlPrimaryKey.setUrn(urn.toString());
                    return mlPrimaryKey;
                  })
              .collect(Collectors.toList()));
    }

    if (mlFeatureTableProperties.hasCustomProperties()) {
      result.setCustomProperties(
          CustomPropertiesMapper.map(mlFeatureTableProperties.getCustomProperties(), entityUrn));
    }

    return result;
  }
}
