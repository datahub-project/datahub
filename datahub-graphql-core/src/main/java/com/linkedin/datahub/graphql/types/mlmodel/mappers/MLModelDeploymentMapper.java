package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLModelDeployment;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelDeploymentKey;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MLModelDeploymentMapper implements ModelMapper<EntityResponse, MLModelDeployment> {

  public static final MLModelDeploymentMapper INSTANCE = new MLModelDeploymentMapper();

  public static MLModelDeployment map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public MLModelDeployment apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final MLModelDeployment result = new MLModelDeployment();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLMODEL_DEPLOYMENT);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<MLModelDeployment> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        ML_MODEL_DEPLOYMENT_KEY_ASPECT_NAME,
        (entity, dataMap) -> mapToMLModelDeploymentKey(entity, dataMap));
    mappingHelper.mapToResult(
        ML_MODEL_DEPLOYMENT_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) -> mapToMLModelDeploymentProperties(context, entity, dataMap, entityUrn));

    return mappingHelper.getResult();
  }

  private static void mapToMLModelDeploymentKey(
      @Nonnull MLModelDeployment mlModelDeployment, DataMap dataMap) {
    MLModelDeploymentKey mlModelDeploymentKey = new MLModelDeploymentKey(dataMap);
    mlModelDeployment.setName(mlModelDeploymentKey.getName());
    DataPlatform platform = new DataPlatform();
    platform.setUrn(mlModelDeploymentKey.getPlatform().toString());
    platform.setType(EntityType.DATA_PLATFORM);
    mlModelDeployment.setPlatform(platform);
  }

  private void mapToMLModelDeploymentProperties(
      @Nullable QueryContext context,
      @Nonnull MLModelDeployment mlModelDeployment,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    com.linkedin.ml.metadata.MLModelDeploymentProperties deploymentProperties =
        new com.linkedin.ml.metadata.MLModelDeploymentProperties(dataMap);

    com.linkedin.datahub.graphql.generated.MLModelDeploymentProperties properties =
        new com.linkedin.datahub.graphql.generated.MLModelDeploymentProperties();

    if (deploymentProperties.hasDescription()) {
      properties.setDescription(deploymentProperties.getDescription());
    }

    mlModelDeployment.setProperties(properties);
  }
}
