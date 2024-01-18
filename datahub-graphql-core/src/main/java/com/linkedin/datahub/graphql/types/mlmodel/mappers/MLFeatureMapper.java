package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLFeatureEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
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
import com.linkedin.metadata.key.MLFeatureKey;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import com.linkedin.ml.metadata.MLFeatureProperties;
import javax.annotation.Nonnull;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLFeatureMapper implements ModelMapper<EntityResponse, MLFeature> {

  public static final MLFeatureMapper INSTANCE = new MLFeatureMapper();

  public static MLFeature map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public MLFeature apply(@Nonnull final EntityResponse entityResponse) {
    final MLFeature result = new MLFeature();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLFEATURE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<MLFeature> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(ML_FEATURE_KEY_ASPECT_NAME, this::mapMLFeatureKey);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(ML_FEATURE_PROPERTIES_ASPECT_NAME, this::mapMLFeatureProperties);
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlFeature, dataMap) -> mlFeature.setStatus(StatusMapper.map(new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

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
        ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setBrowsePathV2(BrowsePathsV2Mapper.map(new BrowsePathsV2(dataMap))));

    return mappingHelper.getResult();
  }

  private void mapMLFeatureKey(@Nonnull MLFeature mlFeature, @Nonnull DataMap dataMap) {
    MLFeatureKey mlFeatureKey = new MLFeatureKey(dataMap);
    mlFeature.setName(mlFeatureKey.getName());
    mlFeature.setFeatureNamespace(mlFeatureKey.getFeatureNamespace());
  }

  private void mapMLFeatureProperties(@Nonnull MLFeature mlFeature, @Nonnull DataMap dataMap) {
    MLFeatureProperties featureProperties = new MLFeatureProperties(dataMap);
    mlFeature.setFeatureProperties(MLFeaturePropertiesMapper.map(featureProperties));
    mlFeature.setProperties(MLFeaturePropertiesMapper.map(featureProperties));
    mlFeature.setDescription(featureProperties.getDescription());
    if (featureProperties.getDataType() != null) {
      mlFeature.setDataType(MLFeatureDataType.valueOf(featureProperties.getDataType().toString()));
    }
  }

  private void mapGlobalTags(MLFeature entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private void mapDomains(@Nonnull MLFeature entity, @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(domains, entity.getUrn()));
  }

  private void mapEditableProperties(MLFeature entity, DataMap dataMap) {
    EditableMLFeatureProperties input = new EditableMLFeatureProperties(dataMap);
    MLFeatureEditableProperties editableProperties = new MLFeatureEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }
}
