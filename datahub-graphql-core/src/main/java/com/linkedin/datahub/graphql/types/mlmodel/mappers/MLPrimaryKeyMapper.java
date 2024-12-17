package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

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
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyEditableProperties;
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
import com.linkedin.metadata.key.MLPrimaryKeyKey;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
import com.linkedin.ml.metadata.MLPrimaryKeyProperties;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema. */
public class MLPrimaryKeyMapper implements ModelMapper<EntityResponse, MLPrimaryKey> {

  public static final MLPrimaryKeyMapper INSTANCE = new MLPrimaryKeyMapper();

  public static MLPrimaryKey map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public MLPrimaryKey apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final MLPrimaryKey result = new MLPrimaryKey();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.MLPRIMARY_KEY);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<MLPrimaryKey> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        ML_PRIMARY_KEY_KEY_ASPECT_NAME, MLPrimaryKeyMapper::mapMLPrimaryKeyKey);
    mappingHelper.mapToResult(
        ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) -> mapMLPrimaryKeyProperties(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setDeprecation(DeprecationMapper.map(context, new Deprecation(dataMap))));

    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (entity, dataMap) -> mapGlobalTags(context, entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(context, DOMAINS_ASPECT_NAME, MLPrimaryKeyMapper::mapDomains);
    mappingHelper.mapToResult(
        ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME, MLPrimaryKeyMapper::mapEditableProperties);
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));
    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), MLPrimaryKey.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private static void mapMLPrimaryKeyKey(MLPrimaryKey mlPrimaryKey, DataMap dataMap) {
    MLPrimaryKeyKey mlPrimaryKeyKey = new MLPrimaryKeyKey(dataMap);
    mlPrimaryKey.setName(mlPrimaryKeyKey.getName());
    mlPrimaryKey.setFeatureNamespace(mlPrimaryKeyKey.getFeatureNamespace());
  }

  private static void mapMLPrimaryKeyProperties(
      @Nullable final QueryContext context,
      MLPrimaryKey mlPrimaryKey,
      DataMap dataMap,
      @Nonnull Urn entityUrn) {
    MLPrimaryKeyProperties primaryKeyProperties = new MLPrimaryKeyProperties(dataMap);
    com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties graphqlProperties =
        MLPrimaryKeyPropertiesMapper.map(context, primaryKeyProperties, entityUrn);
    mlPrimaryKey.setPrimaryKeyProperties(graphqlProperties);
    mlPrimaryKey.setProperties(graphqlProperties);
    mlPrimaryKey.setDescription(primaryKeyProperties.getDescription());
    if (primaryKeyProperties.getDataType() != null) {
      mlPrimaryKey.setDataType(
          MLFeatureDataType.valueOf(primaryKeyProperties.getDataType().toString()));
    }
  }

  private static void mapGlobalTags(
      @Nullable final QueryContext context, MLPrimaryKey entity, DataMap dataMap, Urn entityUrn) {
    GlobalTags globalTags = new GlobalTags(dataMap);
    com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags =
        GlobalTagsMapper.map(context, globalTags, entityUrn);
    entity.setTags(graphQlGlobalTags);
  }

  private static void mapDomains(
      @Nullable final QueryContext context,
      @Nonnull MLPrimaryKey entity,
      @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    entity.setDomain(DomainAssociationMapper.map(context, domains, entity.getUrn()));
  }

  private static void mapEditableProperties(MLPrimaryKey entity, DataMap dataMap) {
    EditableMLPrimaryKeyProperties input = new EditableMLPrimaryKeyProperties(dataMap);
    MLPrimaryKeyEditableProperties editableProperties = new MLPrimaryKeyEditableProperties();
    if (input.hasDescription()) {
      editableProperties.setDescription(input.getDescription());
    }
    entity.setEditableProperties(editableProperties);
  }
}
