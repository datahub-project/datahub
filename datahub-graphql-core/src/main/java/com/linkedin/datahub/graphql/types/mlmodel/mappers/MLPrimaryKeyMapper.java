package com.linkedin.datahub.graphql.types.mlmodel.mappers;

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
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLPrimaryKeyKey;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
import com.linkedin.ml.metadata.MLPrimaryKeyProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLPrimaryKeyMapper implements ModelMapper<EntityResponse, MLPrimaryKey> {

    public static final MLPrimaryKeyMapper INSTANCE = new MLPrimaryKeyMapper();

    public static MLPrimaryKey map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLPrimaryKey apply(@Nonnull final EntityResponse entityResponse) {
        final MLPrimaryKey result = new MLPrimaryKey();
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLPRIMARY_KEY);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        Long lastIngested = SystemMetadataUtils.getLastIngested(aspectMap);
        result.setLastIngested(lastIngested);

        MappingHelper<MLPrimaryKey> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(ML_PRIMARY_KEY_KEY_ASPECT_NAME, this::mapMLPrimaryKeyKey);
        mappingHelper.mapToResult(ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME, this::mapMLPrimaryKeyProperties);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (entity, dataMap) -> this.mapGlobalTags(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (entity, dataMap) ->
            entity.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));
        return mappingHelper.getResult();
    }

    private void mapMLPrimaryKeyKey(MLPrimaryKey mlPrimaryKey, DataMap dataMap) {
        MLPrimaryKeyKey mlPrimaryKeyKey = new MLPrimaryKeyKey(dataMap);
        mlPrimaryKey.setName(mlPrimaryKeyKey.getName());
        mlPrimaryKey.setFeatureNamespace(mlPrimaryKeyKey.getFeatureNamespace());
    }

    private void mapMLPrimaryKeyProperties(MLPrimaryKey mlPrimaryKey, DataMap dataMap) {
        MLPrimaryKeyProperties primaryKeyProperties = new MLPrimaryKeyProperties(dataMap);
        mlPrimaryKey.setPrimaryKeyProperties(MLPrimaryKeyPropertiesMapper.map(primaryKeyProperties));
        mlPrimaryKey.setProperties(MLPrimaryKeyPropertiesMapper.map(primaryKeyProperties));
        mlPrimaryKey.setDescription(primaryKeyProperties.getDescription());
        if (primaryKeyProperties.getDataType() != null) {
            mlPrimaryKey.setDataType(MLFeatureDataType.valueOf(primaryKeyProperties.getDataType().toString()));
        }
    }

    private void mapGlobalTags(MLPrimaryKey entity, DataMap dataMap, Urn entityUrn) {
        GlobalTags globalTags = new GlobalTags(dataMap);
        com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags = GlobalTagsMapper.map(globalTags, entityUrn);
        entity.setTags(graphQlGlobalTags);
    }

    private void mapDomains(@Nonnull MLPrimaryKey entity, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        // Currently we only take the first domain if it exists.
        entity.setDomain(DomainAssociationMapper.map(domains, entity.getUrn()));
    }

    private void mapEditableProperties(MLPrimaryKey entity, DataMap dataMap) {
        EditableMLPrimaryKeyProperties input = new EditableMLPrimaryKeyProperties(dataMap);
        MLPrimaryKeyEditableProperties editableProperties = new MLPrimaryKeyEditableProperties();
        if (input.hasDescription()) {
            editableProperties.setDescription(input.getDescription());
        }
        entity.setEditableProperties(editableProperties);
    }
}
