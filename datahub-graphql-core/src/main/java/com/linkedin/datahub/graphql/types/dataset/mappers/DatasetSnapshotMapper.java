package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.DatasetEditableProperties;
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
import com.linkedin.metadata.entity.NewModelUtils;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaMetadata;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class DatasetSnapshotMapper implements ModelMapper<DatasetSnapshot, Dataset> {

    public static final DatasetSnapshotMapper INSTANCE = new DatasetSnapshotMapper();

    public static Dataset map(@Nonnull final DatasetSnapshot dataset) {
        return INSTANCE.apply(dataset);
    }

    @Override
    public Dataset apply(@Nonnull final DatasetSnapshot dataset) {
        Dataset result = new Dataset();
        result.setUrn(dataset.getUrn().toString());
        result.setType(EntityType.DATASET);
        result.setName(dataset.getUrn().getDatasetNameEntity());
        result.setOrigin(Enum.valueOf(FabricType.class, dataset.getUrn().getOriginEntity().toString()));

        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(dataset.getUrn().getPlatformEntity().toString());
        result.setPlatform(partialPlatform);

        NewModelUtils.getAspectsFromSnapshot(dataset).forEach(nameAspectPair -> {
            RecordTemplate aspect = nameAspectPair.getSecond();
            if (aspect instanceof DatasetProperties) {
                final DatasetProperties gmsProperties = (DatasetProperties) aspect;
                final com.linkedin.datahub.graphql.generated.DatasetProperties properties = new com.linkedin.datahub.graphql.generated.DatasetProperties();
                properties.setDescription(gmsProperties.getDescription());
                result.setDescription(gmsProperties.getDescription());
                properties.setOrigin(FabricType.valueOf(dataset.getUrn().getOriginEntity().toString()));
                if (gmsProperties.hasExternalUrl()) {
                    properties.setExternalUrl(gmsProperties.getExternalUrl().toString());
                }
                if (gmsProperties.hasCustomProperties()) {
                    properties.setCustomProperties(StringMapMapper.map(gmsProperties.getCustomProperties()));
                }
                properties.setName(dataset.getUrn().getDatasetNameEntity()); // TODO: Move to using a display name produced by ingestion soures
                result.setProperties(properties);
                result.setDescription(properties.getDescription());
                if (gmsProperties.hasUri()) {
                    // Deprecated field.
                    result.setUri(gmsProperties.getUri().toString());
                }
            } else if (aspect instanceof DatasetDeprecation) {
                result.setDeprecation(DatasetDeprecationMapper.map((DatasetDeprecation) aspect));
            } else if (aspect instanceof InstitutionalMemory) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map((InstitutionalMemory) aspect));
            } else if (aspect instanceof Ownership) {
                result.setOwnership(OwnershipMapper.map((Ownership) aspect));
            } else if (aspect instanceof SchemaMetadata) {
                result.setSchema(
                    SchemaMapper.map((SchemaMetadata) aspect)
                );
            } else if (aspect instanceof Status) {
              result.setStatus(StatusMapper.map((Status) aspect));
            } else if (aspect instanceof GlobalTags) {
              result.setGlobalTags(GlobalTagsMapper.map((GlobalTags) aspect));
              result.setTags(GlobalTagsMapper.map((GlobalTags) aspect));
            } else if (aspect instanceof EditableSchemaMetadata) {
              result.setEditableSchemaMetadata(EditableSchemaMetadataMapper.map((EditableSchemaMetadata) aspect));
            } else if (aspect instanceof GlossaryTerms) {
              result.setGlossaryTerms(GlossaryTermsMapper.map((GlossaryTerms) aspect));
            } else if (aspect instanceof EditableDatasetProperties) {
                final EditableDatasetProperties editableDatasetProperties = (EditableDatasetProperties) aspect;
                final DatasetEditableProperties editableProperties = new DatasetEditableProperties();
                editableProperties.setDescription(editableDatasetProperties.getDescription());
                result.setEditableProperties(editableProperties);
            } else if (aspect instanceof ViewProperties) {
                final ViewProperties properties = (ViewProperties) aspect;
                final com.linkedin.datahub.graphql.generated.ViewProperties graphqlProperties = new com.linkedin.datahub.graphql.generated.ViewProperties();
                graphqlProperties.setMaterialized(properties.isMaterialized());
                graphqlProperties.setLanguage(properties.getViewLanguage());
                graphqlProperties.setLogic(properties.getViewLogic());
                result.setViewProperties(graphqlProperties);
            }
        });
        return result;
    }
}
