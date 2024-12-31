package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
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
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLModelGroupMapper implements ModelMapper<EntityResponse, MLModelGroup> {

  public static final MLModelGroupMapper INSTANCE = new MLModelGroupMapper();

  public static MLModelGroup map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public MLModelGroup apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
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
            mlModelGroup.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        ML_MODEL_GROUP_KEY_ASPECT_NAME, MLModelGroupMapper::mapToMLModelGroupKey);
    mappingHelper.mapToResult(
        ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) -> mapToMLModelGroupProperties(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setDeprecation(DeprecationMapper.map(context, new Deprecation(dataMap))));

    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (entity, dataMap) -> MLModelGroupMapper.mapGlobalTags(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(context, DOMAINS_ASPECT_NAME, MLModelGroupMapper::mapDomains);
    mappingHelper.mapToResult(
        ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME, MLModelGroupMapper::mapEditableProperties);
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (mlModelGroup, dataMap) ->
            mlModelGroup.setBrowsePathV2(
                BrowsePathsV2Mapper.map(context, new BrowsePathsV2(dataMap))));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((mlModelGroup, dataMap) ->
            mlModelGroup.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), MLModelGroup.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private static void mapToMLModelGroupKey(MLModelGroup mlModelGroup, DataMap dataMap) {
    MLModelGroupKey mlModelGroupKey = new MLModelGroupKey(dataMap);
    mlModelGroup.setName(mlModelGroupKey.getName());
    mlModelGroup.setOrigin(FabricType.valueOf(mlModelGroupKey.getOrigin().toString()));
    DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(mlModelGroupKey.getPlatform().toString());
    mlModelGroup.setPlatform(partialPlatform);
  }

  private static void mapToMLModelGroupProperties(
      @Nullable final QueryContext context,
      MLModelGroup mlModelGroup,
      DataMap dataMap,
      @Nonnull Urn entityUrn) {
    MLModelGroupProperties modelGroupProperties = new MLModelGroupProperties(dataMap);
    mlModelGroup.setProperties(
        MLModelGroupPropertiesMapper.map(context, modelGroupProperties, entityUrn));
    if (modelGroupProperties.getDescription() != null) {
      mlModelGroup.setDescription(modelGroupProperties.getDescription());
    }
  }

  private static void mapGlobalTags(
      @Nullable final QueryContext context, MLModelGroup entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(context, globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private static void mapDomains(
      @Nullable final QueryContext context,
      @Nonnull MLModelGroup entity,
      @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(context, domains, entity.getUrn()));
  }

  private static void mapEditableProperties(MLModelGroup entity, DataMap dataMap) {
    EditableMLModelGroupProperties input = new EditableMLModelGroupProperties(dataMap);
    MLModelGroupEditableProperties editableProperties = new MLModelGroupEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }
}
