package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLHyperParamMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLMetricMapper;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.ml.metadata.MLTrainingRunProperties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
@Slf4j
public class DataProcessInstanceMapper implements ModelMapper<EntityResponse, DataProcessInstance> {

  public static final DataProcessInstanceMapper INSTANCE = new DataProcessInstanceMapper();

  public static DataProcessInstance map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  private void mapContainers(
      @Nullable final QueryContext context,
      @Nonnull DataProcessInstance dataProcessInstance,
      @Nonnull DataMap dataMap) {
    final com.linkedin.container.Container gmsContainer =
        new com.linkedin.container.Container(dataMap);
    dataProcessInstance.setContainer(
        com.linkedin.datahub.graphql.generated.Container.builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
  }

  @Override
  public DataProcessInstance apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataProcessInstance result = new DataProcessInstance();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_PROCESS_INSTANCE);

    Urn entityUrn = entityResponse.getUrn();
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataProcessInstance> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME,
        (dataProcessInstance, dataMap) ->
            mapDataProcessInstanceProperties(context, dataProcessInstance, dataMap, entityUrn));
    mappingHelper.mapToResult(
        ML_TRAINING_RUN_PROPERTIES_ASPECT_NAME,
        (dataProcessInstance, dataMap) ->
            mapTrainingRunProperties(context, dataProcessInstance, dataMap));
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataProcessInstance, dataMap) -> {
          DataPlatformInstance dataPlatformInstance = new DataPlatformInstance(dataMap);
          com.linkedin.datahub.graphql.generated.DataPlatformInstance value =
              DataPlatformInstanceAspectMapper.map(context, dataPlatformInstance);
          dataProcessInstance.setPlatform(value.getPlatform());
          dataProcessInstance.setDataPlatformInstance(value);
        });
    mappingHelper.mapToResult(
        SUB_TYPES_ASPECT_NAME,
        (dataProcessInstance, dataMap) ->
            dataProcessInstance.setSubTypes(SubTypesMapper.map(context, new SubTypes(dataMap))));
    mappingHelper.mapToResult(
        CONTAINER_ASPECT_NAME,
        (dataProcessInstance, dataMap) -> mapContainers(context, dataProcessInstance, dataMap));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (dataProcessInstance, dataMap) ->
            dataProcessInstance.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DATA_PROCESS_INSTANCE_RELATIONSHIPS_ASPECT_NAME,
        (dataProcessInstance, dataMap) ->
            mapDataProcessInstanceRelationships(context, dataProcessInstance, dataMap));

    return mappingHelper.getResult();
  }

  private void mapTrainingRunProperties(
      @Nonnull QueryContext context, @Nonnull DataProcessInstance dpi, @Nonnull DataMap dataMap) {
    MLTrainingRunProperties trainingProperties = new MLTrainingRunProperties(dataMap);

    com.linkedin.datahub.graphql.generated.MLTrainingRunProperties properties =
        new com.linkedin.datahub.graphql.generated.MLTrainingRunProperties();
    if (trainingProperties.hasId()) {
      properties.setId(trainingProperties.getId());
    }
    if (trainingProperties.hasOutputUrls()) {
      properties.setOutputUrls(
          trainingProperties.getOutputUrls().stream()
              .map(url -> url.toString())
              .collect(Collectors.toList()));
    }
    if (trainingProperties.getHyperParams() != null) {
      properties.setHyperParams(
          trainingProperties.getHyperParams().stream()
              .map(param -> MLHyperParamMapper.map(context, param))
              .collect(Collectors.toList()));
    }
    if (trainingProperties.getTrainingMetrics() != null) {
      properties.setTrainingMetrics(
          trainingProperties.getTrainingMetrics().stream()
              .map(metric -> MLMetricMapper.map(context, metric))
              .collect(Collectors.toList()));
    }
    if (trainingProperties.hasId()) {
      properties.setId(trainingProperties.getId());
    }
    dpi.setMlTrainingRunProperties(properties);
  }

  private void mapDataProcessInstanceProperties(
      @Nullable QueryContext context,
      @Nonnull DataProcessInstance dpi,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    DataProcessInstanceProperties dataProcessInstanceProperties =
        new DataProcessInstanceProperties(dataMap);

    com.linkedin.datahub.graphql.generated.DataProcessInstanceProperties properties =
        new com.linkedin.datahub.graphql.generated.DataProcessInstanceProperties();

    dpi.setName(dataProcessInstanceProperties.getName());
    properties.setName(dataProcessInstanceProperties.getName());
    if (dataProcessInstanceProperties.hasExternalUrl()) {
      dpi.setExternalUrl(dataProcessInstanceProperties.getExternalUrl().toString());
      properties.setExternalUrl(dataProcessInstanceProperties.getExternalUrl().toString());
    }
    if (dataProcessInstanceProperties.hasCustomProperties()) {
      properties.setCustomProperties(
          CustomPropertiesMapper.map(
              dataProcessInstanceProperties.getCustomProperties(), entityUrn));
    }
    dpi.setCreated(AuditStampMapper.map(context, dataProcessInstanceProperties.getCreated()));
    properties.setCreated(
        AuditStampMapper.map(context, dataProcessInstanceProperties.getCreated()));
    dpi.setProperties(properties);
  }

  private void mapDataProcessInstanceRelationships(
      @Nullable QueryContext context, @Nonnull DataProcessInstance dpi, @Nonnull DataMap dataMap) {
    DataProcessInstanceRelationships dataProcessInstanceRelationships =
        new DataProcessInstanceRelationships(dataMap);

    if (dataProcessInstanceRelationships.getParentTemplate() != null) {
      dpi.setParentTemplate(
          UrnToEntityMapper.map(context, dataProcessInstanceRelationships.getParentTemplate()));
    }
  }
}
