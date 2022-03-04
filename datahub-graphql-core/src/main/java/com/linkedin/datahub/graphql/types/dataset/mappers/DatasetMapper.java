package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetEditableProperties;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaMetadata;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps GMS response objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
@Slf4j
public class DatasetMapper implements ModelMapper<EntityResponse, Dataset> {

    public static final DatasetMapper INSTANCE = new DatasetMapper();

    public static Dataset map(@Nonnull final EntityResponse dataset) {
        return INSTANCE.apply(dataset);
    }

    @Override
    public Dataset apply(@Nonnull final EntityResponse entityResponse) {
        Dataset result = new Dataset();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATASET);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<Dataset> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(DATASET_KEY_ASPECT_NAME, this::mapDatasetKey);
        mappingHelper.mapToResult(DATASET_PROPERTIES_ASPECT_NAME, this::mapDatasetProperties);
        mappingHelper.mapToResult(DATASET_DEPRECATION_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDeprecation(DatasetDeprecationMapper.map(new DatasetDeprecation(dataMap))));
        mappingHelper.mapToResult(SCHEMA_METADATA_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setSchema(SchemaMapper.map(new SchemaMetadata(dataMap))));
        mappingHelper.mapToResult(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME, this::mapEditableDatasetProperties);
        mappingHelper.mapToResult(VIEW_PROPERTIES_ASPECT_NAME, this::mapViewProperties);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, this::mapGlobalTags);
        mappingHelper.mapToResult(EDITABLE_SCHEMA_METADATA_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setEditableSchemaMetadata(EditableSchemaMetadataMapper.map(new EditableSchemaMetadata(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap))));
        mappingHelper.mapToResult(CONTAINER_ASPECT_NAME, this::mapContainers);
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapDatasetKey(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final DatasetKey gmsKey = new DatasetKey(dataMap);
        dataset.setName(gmsKey.getName());
        dataset.setOrigin(FabricType.valueOf(gmsKey.getOrigin().toString()));
        dataset.setPlatform(DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(gmsKey.getPlatform().toString()).build());
    }

    private void mapDatasetProperties(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final DatasetProperties gmsProperties = new DatasetProperties(dataMap);
        final com.linkedin.datahub.graphql.generated.DatasetProperties properties =
            new com.linkedin.datahub.graphql.generated.DatasetProperties();
        properties.setDescription(gmsProperties.getDescription());
        dataset.setDescription(gmsProperties.getDescription());
        properties.setOrigin(dataset.getOrigin());
        if (gmsProperties.getExternalUrl() != null) {
            properties.setExternalUrl(gmsProperties.getExternalUrl().toString());
        }
        properties.setCustomProperties(StringMapMapper.map(gmsProperties.getCustomProperties()));
        if (gmsProperties.getName() != null) {
            properties.setName(gmsProperties.getName());
        } else {
            properties.setName(dataset.getName());
        }
        properties.setQualifiedName(gmsProperties.getQualifiedName());
        dataset.setProperties(properties);
        dataset.setDescription(properties.getDescription());
        if (gmsProperties.getUri() != null) {
            dataset.setUri(gmsProperties.getUri().toString());
        }
    }

    private void mapEditableDatasetProperties(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties(dataMap);
        final DatasetEditableProperties editableProperties = new DatasetEditableProperties();
        editableProperties.setDescription(editableDatasetProperties.getDescription());
        dataset.setEditableProperties(editableProperties);
    }

    private void mapViewProperties(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final ViewProperties properties = new ViewProperties(dataMap);
        final com.linkedin.datahub.graphql.generated.ViewProperties graphqlProperties =
            new com.linkedin.datahub.graphql.generated.ViewProperties();
        graphqlProperties.setMaterialized(properties.isMaterialized());
        graphqlProperties.setLanguage(properties.getViewLanguage());
        graphqlProperties.setLogic(properties.getViewLogic());
        dataset.setViewProperties(graphqlProperties);
    }

    private void mapGlobalTags(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(dataMap));
        dataset.setGlobalTags(globalTags);
        dataset.setTags(globalTags);
    }

    private void mapContainers(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(dataMap);
        dataset.setContainer(Container
            .builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
    }

    private void mapDomains(@Nonnull Dataset dataset, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        // Currently we only take the first domain if it exists.
        if (domains.getDomains().size() > 0) {
            dataset.setDomain(Domain.builder()
                .setType(EntityType.DOMAIN)
                .setUrn(domains.getDomains().get(0).toString()).build());
        }
    }
}
