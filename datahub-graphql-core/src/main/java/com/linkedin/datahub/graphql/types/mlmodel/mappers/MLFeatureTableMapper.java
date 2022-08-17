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
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLFeatureTableEditableProperties;
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
import com.linkedin.metadata.key.MLFeatureTableKey;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import com.linkedin.ml.metadata.MLFeatureTableProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLFeatureTableMapper implements ModelMapper<EntityResponse, MLFeatureTable> {

    public static final MLFeatureTableMapper INSTANCE = new MLFeatureTableMapper();

    public static MLFeatureTable map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLFeatureTable apply(@Nonnull final EntityResponse entityResponse) {
        final MLFeatureTable result = new MLFeatureTable();
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLFEATURE_TABLE);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        Long lastIngested = SystemMetadataUtils.getLastIngested(aspectMap);
        result.setLastIngested(lastIngested);

        MappingHelper<MLFeatureTable> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
        mappingHelper.mapToResult(ML_FEATURE_TABLE_KEY_ASPECT_NAME, this::mapMLFeatureTableKey);
        mappingHelper.mapToResult(ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME, (entity, dataMap) -> this.mapMLFeatureTableProperties(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (entity, dataMap) -> this.mapGlobalTags(entity, dataMap, entityUrn));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (entity, dataMap) ->
            entity.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapMLFeatureTableKey(@Nonnull MLFeatureTable mlFeatureTable, @Nonnull DataMap dataMap) {
        MLFeatureTableKey mlFeatureTableKey = new MLFeatureTableKey(dataMap);
        mlFeatureTable.setName(mlFeatureTableKey.getName());
        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(mlFeatureTableKey.getPlatform().toString());
        mlFeatureTable.setPlatform(partialPlatform);
    }

    private void mapMLFeatureTableProperties(@Nonnull MLFeatureTable mlFeatureTable, @Nonnull DataMap dataMap, Urn entityUrn) {
        MLFeatureTableProperties featureTableProperties = new MLFeatureTableProperties(dataMap);
        mlFeatureTable.setFeatureTableProperties(MLFeatureTablePropertiesMapper.map(featureTableProperties, entityUrn));
        mlFeatureTable.setProperties(MLFeatureTablePropertiesMapper.map(featureTableProperties, entityUrn));
        mlFeatureTable.setDescription(featureTableProperties.getDescription());
    }

    private void mapGlobalTags(MLFeatureTable entity, DataMap dataMap, Urn entityUrn) {
        GlobalTags globalTags = new GlobalTags(dataMap);
        com.linkedin.datahub.graphql.generated.GlobalTags graphQlGlobalTags = GlobalTagsMapper.map(globalTags, entityUrn);
        entity.setTags(graphQlGlobalTags);
    }

    private void mapDomains(@Nonnull MLFeatureTable entity, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        // Currently we only take the first domain if it exists.
        entity.setDomain(DomainAssociationMapper.map(domains, entity.getUrn()));
    }

    private void mapEditableProperties(MLFeatureTable entity, DataMap dataMap) {
        EditableMLFeatureTableProperties input = new EditableMLFeatureTableProperties(dataMap);
        MLFeatureTableEditableProperties editableProperties = new MLFeatureTableEditableProperties();
        if (input.hasDescription()) {
            editableProperties.setDescription(input.getDescription());
        }
        entity.setEditableProperties(editableProperties);
    }
}
