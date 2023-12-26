package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLModelGroupEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import javax.annotation.Nonnull;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLModelGroupMapper implements ModelMapper<EntityResponse, MLModelGroup> {

  public static final MLModelGroupMapper INSTANCE = new MLModelGroupMapper();

  public static MLModelGroup map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public MLModelGroup apply(@Nonnull final EntityResponse entityResponse) {
    final MLModelGroup result = new MLModelGroup();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLMODEL_GROUP);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<MLModelGroup> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(ML_MODEL_GROUP_KEY_ASPECT_NAME, this::mapToMLModelGroupKey);
    mappingHelper.mapToResult(
        ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME, this::mapToMLModelGroupProperties);
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlModelGroup, dataMap) -> mlModelGroup.setStatus(StatusMapper.map(new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (entity, dataMap) -> this.mapGlobalTags(entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setGlossaryTerms(
                GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
    mappingHelper.mapToResult(
        ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setBrowsePathV2(BrowsePathsV2Mapper.map(new BrowsePathsV2(dataMap))));

    return mappingHelper.getResult();
  }

  private void mapToMLModelGroupKey(MLModelGroup mlModelGroup, DataMap dataMap) {
    MLModelGroupKey mlModelGroupKey = new MLModelGroupKey(dataMap);
    mlModelGroup.setName(mlModelGroupKey.getName());
    mlModelGroup.setOrigin(FabricType.valueOf(mlModelGroupKey.getOrigin().toString()));
    DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(mlModelGroupKey.getPlatform().toString());
    mlModelGroup.setPlatform(partialPlatform);
  }

  private void mapToMLModelGroupProperties(MLModelGroup mlModelGroup, DataMap dataMap) {
    MLModelGroupProperties modelGroupProperties = new MLModelGroupProperties(dataMap);
    mlModelGroup.setProperties(MLModelGroupPropertiesMapper.map(modelGroupProperties));
    if (modelGroupProperties.getDescription() != null) {
      mlModelGroup.setDescription(modelGroupProperties.getDescription());
    }
  }

  private void mapGlobalTags(MLModelGroup entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private void mapDomains(@Nonnull MLModelGroup entity, @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(domains, entity.getUrn()));
  }

  private void mapEditableProperties(MLModelGroup entity, DataMap dataMap) {
    EditableMLModelGroupProperties input = new EditableMLModelGroupProperties(dataMap);
    MLModelGroupEditableProperties editableProperties = new MLModelGroupEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }
}
