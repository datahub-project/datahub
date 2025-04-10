package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
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
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLFeatureKey;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import com.linkedin.ml.metadata.MLFeatureProperties;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLFeatureMapper implements ModelMapper<EntityResponse, MLFeature> {

  public static final MLFeatureMapper INSTANCE = new MLFeatureMapper();

  public static MLFeature map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public MLFeature apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final MLFeature result = new MLFeature();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLFEATURE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<MLFeature> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(ML_FEATURE_KEY_ASPECT_NAME, MLFeatureMapper::mapMLFeatureKey);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        ML_FEATURE_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) -> mapMLFeatureProperties(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlFeature, dataMap) ->
            mlFeature.setDeprecation(DeprecationMapper.map(context, new Deprecation(dataMap))));

    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (entity, dataMap) -> mapGlobalTags(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(context, DOMAINS_ASPECT_NAME, MLFeatureMapper::mapDomains);
    mappingHelper.mapToResult(
        ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME, MLFeatureMapper::mapEditableProperties);
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setBrowsePathV2(BrowsePathsV2Mapper.map(context, new BrowsePathsV2(dataMap))));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((mlFeature, dataMap) ->
            mlFeature.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), MLFeature.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private static void mapMLFeatureKey(@Nonnull MLFeature mlFeature, @Nonnull DataMap dataMap) {
    MLFeatureKey mlFeatureKey = new MLFeatureKey(dataMap);
    mlFeature.setName(mlFeatureKey.getName());
    mlFeature.setFeatureNamespace(mlFeatureKey.getFeatureNamespace());
  }

  private static void mapMLFeatureProperties(
      @Nullable final QueryContext context,
      @Nonnull MLFeature mlFeature,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    MLFeatureProperties featureProperties = new MLFeatureProperties(dataMap);
    com.linkedin.datahub.graphql.generated.MLFeatureProperties graphqlProperties =
        MLFeaturePropertiesMapper.map(context, featureProperties, entityUrn);
    mlFeature.setFeatureProperties(graphqlProperties);
    mlFeature.setProperties(graphqlProperties);
    mlFeature.setDescription(featureProperties.getDescription());
    if (featureProperties.getDataType() != null) {
      mlFeature.setDataType(MLFeatureDataType.valueOf(featureProperties.getDataType().toString()));
    }
  }

  private static void mapGlobalTags(
      @Nullable final QueryContext context, MLFeature entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(context, globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private static void mapDomains(
      @Nullable final QueryContext context, @Nonnull MLFeature entity, @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(context, domains, entity.getUrn()));
  }

  private static void mapEditableProperties(MLFeature entity, DataMap dataMap) {
    EditableMLFeatureProperties input = new EditableMLFeatureProperties(dataMap);
    MLFeatureEditableProperties editableProperties = new MLFeatureEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }
}
