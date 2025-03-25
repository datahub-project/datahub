package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLModelDeployment;
import com.linkedin.datahub.graphql.generated.MLModelDeploymentProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelDeploymentKey;
import javax.annotation.Nonnull;

public class MLModelDeploymentMapper implements ModelMapper<EntityResponse, MLModelDeployment> {

  public static final MLModelDeploymentMapper INSTANCE = new MLModelDeploymentMapper();

  public static MLModelDeployment map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(null, entityResponse);
  }

  @Override
  public MLModelDeployment apply(
      @Nonnull final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final MLModelDeployment result = new MLModelDeployment();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.valueOf("MLMODEL_DEPLOYMENT"));

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    for (EnvelopedAspect aspect : aspectMap.values()) {
      if (aspect.getValue() != null) {
        RecordTemplate recordTemplate = aspect.getValue();
        if (recordTemplate instanceof MLModelDeploymentKey) {
          mapMLModelDeploymentKey((MLModelDeploymentKey) recordTemplate, result);
        } else if (recordTemplate instanceof com.linkedin.ml.metadata.MLModelDeploymentProperties) {
          mapMLModelDeploymentProperties(
              (com.linkedin.ml.metadata.MLModelDeploymentProperties) recordTemplate, result);
        }
      }
    }

    return result;
  }

  private void mapMLModelDeploymentKey(
      @Nonnull MLModelDeploymentKey mlModelDeploymentKey,
      @Nonnull MLModelDeployment mlModelDeployment) {
    mlModelDeployment.setName(mlModelDeploymentKey.getName());
    mlModelDeployment.setPlatform(mlModelDeploymentKey.getPlatform().toString());
  }

  private void mapMLModelDeploymentProperties(
      @Nonnull com.linkedin.ml.metadata.MLModelDeploymentProperties properties,
      @Nonnull MLModelDeployment mlModelDeployment) {
    MLModelDeploymentProperties props = new MLModelDeploymentProperties();
    if (properties.hasDescription()) {
      props.setDescription(properties.getDescription());
    }
    mlModelDeployment.setProperties(props);
  }
}
