package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetEditableProperties;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.dataset.ViewProperties;
import com.linkedin.entity.EntityResponse;
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
        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (DATASET_KEY_ASPECT_NAME.equals(name)) {
                final DatasetKey gmsKey = new DatasetKey(data);
                result.setName(gmsKey.getName());
                result.setOrigin(FabricType.valueOf(gmsKey.getOrigin().toString()));
                result.setPlatform(DataPlatform.builder()
                    .setType(EntityType.DATA_PLATFORM)
                    .setUrn(gmsKey.getPlatform().toString()).build());
            } else if (DATASET_PROPERTIES_ASPECT_NAME.equals(name)) {
                final DatasetProperties gmsProperties = new DatasetProperties(data);
                final com.linkedin.datahub.graphql.generated.DatasetProperties properties = new com.linkedin.datahub.graphql.generated.DatasetProperties();
                properties.setDescription(gmsProperties.getDescription());
                result.setDescription(gmsProperties.getDescription());
                //properties.setOrigin(FabricType.valueOf(entityResponse.getUrn().getOriginEntity().toString()));
                if (gmsProperties.getExternalUrl() != null) {
                    properties.setExternalUrl(gmsProperties.getExternalUrl().toString());
                }
                properties.setCustomProperties(StringMapMapper.map(gmsProperties.getCustomProperties()));
                //properties.setName(entityResponse.getUrn().getDatasetNameEntity()); // TODO: Move to using a display name produced by ingestion soures
                result.setProperties(properties);
                result.setDescription(properties.getDescription());
                if (gmsProperties.getUri() != null) {
                    // Deprecated field.
                    result.setUri(gmsProperties.getUri().toString());
                }
            } else if (DATASET_DEPRECATION_ASPECT_NAME.equals(name)) {
                result.setDeprecation(DatasetDeprecationMapper.map(new DatasetDeprecation(data)));
            } else if (SCHEMA_METADATA_ASPECT_NAME.equals(name)) {
                result.setSchema(SchemaMapper.map(new SchemaMetadata(data)));
            } else if (EDITABLE_DATASET_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties(data);
                final DatasetEditableProperties editableProperties = new DatasetEditableProperties();
                editableProperties.setDescription(editableDatasetProperties.getDescription());
                result.setEditableProperties(editableProperties);
            } else if (VIEW_PROPERTIES_ASPECT_NAME.equals(name)) {
                final ViewProperties properties = new ViewProperties(data);
                final com.linkedin.datahub.graphql.generated.ViewProperties graphqlProperties =
                    new com.linkedin.datahub.graphql.generated.ViewProperties();
                graphqlProperties.setMaterialized(properties.isMaterialized());
                graphqlProperties.setLanguage(properties.getViewLanguage());
                graphqlProperties.setLogic(properties.getViewLogic());
                result.setViewProperties(graphqlProperties);
            } else if (INSTITUTIONAL_MEMORY_ASPECT_NAME.equals(name)) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(data)));
            } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(new Ownership(data)));
            } else if (STATUS_ASPECT_NAME.equals(name)) {
                result.setStatus(StatusMapper.map(new Status(data)));
            } else if (GLOBAL_TAGS_ASPECT_NAME.equals(name)) {
                com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(data));
                result.setGlobalTags(globalTags);
                result.setTags(globalTags);
            } else if (EDITABLE_SCHEMA_METADATA_ASPECT_NAME.equals(name)) {
                result.setEditableSchemaMetadata(EditableSchemaMetadataMapper.map(new EditableSchemaMetadata(data)));
            } else if (GLOSSARY_TERMS_ASPECT_NAME.equals(name)) {
                result.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(data)));
            }
        });
        return result;
    }
}
