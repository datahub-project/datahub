package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.datahub.graphql.VersionedAspectKey;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.DatasetEditableProperties;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.EditableSchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.util.Pair;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class AspectMapper implements ModelMapper<Pair<VersionedAspectKey, EntityResponse>, Aspect> {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(@Nonnull final Pair<VersionedAspectKey, EntityResponse> responsePair) {
    return INSTANCE.apply(responsePair);
  }

  @Override
  public Aspect apply(@Nonnull final Pair<VersionedAspectKey, EntityResponse> responsePair) {
    EntityResponse response = responsePair.getSecond();
    EnvelopedAspect aspect = response.getAspects().get(responsePair.getFirst().getAspectName());
    switch (aspect.getName()) {
      case SCHEMA_METADATA_ASPECT_NAME:
        return SchemaMetadataMapper.map(aspect);
      case GLOBAL_TAGS_ASPECT_NAME:
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper
            .map(new GlobalTags(aspect.getValue().data()));
        globalTags.setVersion(aspect.getVersion());
        return globalTags;
      case GLOSSARY_TERMS_ASPECT_NAME:
        com.linkedin.datahub.graphql.generated.GlossaryTerms glossaryTerms = GlossaryTermsMapper
            .map(new GlossaryTerms(aspect.getValue().data()));
        glossaryTerms.setVersion(aspect.getVersion());
        return glossaryTerms;
      case DATASET_PROPERTIES_ASPECT_NAME:
        return mapDatasetProperties(response, aspect);
      case EDITABLE_DATASET_PROPERTIES_ASPECT_NAME:
        DatasetEditableProperties datasetEditableProperties = new DatasetEditableProperties();
        EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties(aspect.getValue().data());
        datasetEditableProperties.setDescription(editableDatasetProperties.getDescription());
        datasetEditableProperties.setVersion(aspect.getVersion());
        return datasetEditableProperties;
      case EDITABLE_SCHEMA_METADATA_ASPECT_NAME:
        com.linkedin.datahub.graphql.generated.EditableSchemaMetadata editableSchemaMetadata = EditableSchemaMetadataMapper
            .map(new EditableSchemaMetadata(aspect.getValue().data()));
        editableSchemaMetadata.setVersion(aspect.getVersion());
        return editableSchemaMetadata;
      case INSTITUTIONAL_MEMORY_ASPECT_NAME:
        com.linkedin.datahub.graphql.generated.InstitutionalMemory institutionalMemory = InstitutionalMemoryMapper
            .map(new InstitutionalMemory(aspect.getValue().data()));
        institutionalMemory.setVersion(aspect.getVersion());
        return institutionalMemory;
      case OWNERSHIP_ASPECT_NAME:
        com.linkedin.datahub.graphql.generated.Ownership ownership = OwnershipMapper
            .map(new Ownership(aspect.getValue().data()));
        ownership.setVersion(aspect.getVersion());
        return ownership;
      default:
        return null;
    }
  }

  private com.linkedin.datahub.graphql.generated.DatasetProperties mapDatasetProperties(EntityResponse response,
      EnvelopedAspect aspect) {
    DatasetKey datasetKey = (DatasetKey) EntityKeyUtils.convertUrnToEntityKey(response.getUrn(),
        new DatasetKey().schema());
    DatasetProperties datasetProperties = new DatasetProperties(aspect.getValue().data());
    com.linkedin.datahub.graphql.generated.DatasetProperties graphQlProperties =
        new com.linkedin.datahub.graphql.generated.DatasetProperties();
    graphQlProperties.setOrigin(FabricType.valueOf(datasetKey.getOrigin().toString()));
    graphQlProperties.setDescription(datasetProperties.getDescription());
    if (datasetProperties.getExternalUrl() != null) {
      graphQlProperties.setExternalUrl(datasetProperties.getExternalUrl().toString());
    }
    graphQlProperties.setCustomProperties(StringMapMapper.map(datasetProperties.getCustomProperties()));
    if (datasetProperties.getName() != null) {
      graphQlProperties.setName(datasetProperties.getName());
    } else {
      graphQlProperties.setName(datasetKey.getName());
    }
    graphQlProperties.setQualifiedName(datasetProperties.getQualifiedName());
    return graphQlProperties;
  }
}
