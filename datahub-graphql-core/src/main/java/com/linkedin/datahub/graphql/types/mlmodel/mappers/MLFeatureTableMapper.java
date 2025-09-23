package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.application.Applications;
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
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLFeatureTableEditableProperties;
import com.linkedin.datahub.graphql.types.application.ApplicationAssociationMapper;
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
import com.linkedin.metadata.key.MLFeatureTableKey;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import com.linkedin.ml.metadata.MLFeatureTableProperties;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLFeatureTableMapper implements ModelMapper<EntityResponse, MLFeatureTable> {

  public static final MLFeatureTableMapper INSTANCE = new MLFeatureTableMapper();

  public static MLFeatureTable map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public MLFeatureTable apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final MLFeatureTable result = new MLFeatureTable();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLFEATURE_TABLE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<MLFeatureTable> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (mlFeatureTable, dataMap) ->
            mlFeatureTable.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(ML_FEATURE_TABLE_KEY_ASPECT_NAME, this::mapMLFeatureTableKey);
    mappingHelper.mapToResult(
        ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) -> mapMLFeatureTableProperties(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (mlFeatureTable, dataMap) ->
            mlFeatureTable.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlFeatureTable, dataMap) ->
            mlFeatureTable.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlFeatureTable, dataMap) ->
            mlFeatureTable.setDeprecation(
                DeprecationMapper.map(context, new Deprecation(dataMap))));

    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (entity, dataMap) -> mapGlobalTags(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(context, DOMAINS_ASPECT_NAME, MLFeatureTableMapper::mapDomains);
    mappingHelper.mapToResult(
        ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
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
        ((mlFeatureTable, dataMap) ->
            mlFeatureTable.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));
    mappingHelper.mapToResult(
        APPLICATION_MEMBERSHIP_ASPECT_NAME,
        (mlFeatureTable, dataMap) -> mapApplicationAssociation(context, mlFeatureTable, dataMap));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), MLFeatureTable.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private void mapMLFeatureTableKey(
      @Nonnull MLFeatureTable mlFeatureTable, @Nonnull DataMap dataMap) {
    MLFeatureTableKey mlFeatureTableKey = new MLFeatureTableKey(dataMap);
    mlFeatureTable.setName(mlFeatureTableKey.getName());
    DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(mlFeatureTableKey.getPlatform().toString());
    mlFeatureTable.setPlatform(partialPlatform);
  }

  private static void mapMLFeatureTableProperties(
      @Nullable final QueryContext context,
      @Nonnull MLFeatureTable mlFeatureTable,
      @Nonnull DataMap dataMap,
      Urn entityUrn) {
    MLFeatureTableProperties featureTableProperties = new MLFeatureTableProperties(dataMap);
    com.linkedin.datahub.graphql.generated.MLFeatureTableProperties graphqlProperties =
        MLFeatureTablePropertiesMapper.map(context, featureTableProperties, entityUrn);
    mlFeatureTable.setFeatureTableProperties(graphqlProperties);
    mlFeatureTable.setProperties(graphqlProperties);
    mlFeatureTable.setDescription(featureTableProperties.getDescription());
  }

  private static void mapGlobalTags(
      @Nullable final QueryContext context, MLFeatureTable entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(context, globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private static void mapDomains(
      @Nullable final QueryContext context,
      @Nonnull MLFeatureTable entity,
      @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(context, domains, entity.getUrn()));
  }

  private void mapEditableProperties(MLFeatureTable entity, DataMap dataMap) {
    EditableMLFeatureTableProperties input = new EditableMLFeatureTableProperties(dataMap);
    MLFeatureTableEditableProperties editableProperties = new MLFeatureTableEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }

  private static void mapApplicationAssociation(
      @Nullable final QueryContext context,
      @Nonnull MLFeatureTable mlFeatureTable,
      @Nonnull DataMap dataMap) {
    final Applications applications = new Applications(dataMap);
    mlFeatureTable.setApplication(
        ApplicationAssociationMapper.map(context, applications, mlFeatureTable.getUrn()));
  }
}
