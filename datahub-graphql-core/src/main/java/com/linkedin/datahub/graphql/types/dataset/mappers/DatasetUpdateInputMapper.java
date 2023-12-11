package com.linkedin.datahub.graphql.types.dataset.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.UpdateMappingHelper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DatasetUpdateInputMapper
    implements InputModelMapper<DatasetUpdateInput, Collection<MetadataChangeProposal>, Urn> {

  public static final DatasetUpdateInputMapper INSTANCE = new DatasetUpdateInputMapper();

  public static Collection<MetadataChangeProposal> map(
      @Nonnull final DatasetUpdateInput datasetUpdateInput, @Nonnull final Urn actor) {
    return INSTANCE.apply(datasetUpdateInput, actor);
  }

  @Override
  public Collection<MetadataChangeProposal> apply(
      @Nonnull final DatasetUpdateInput datasetUpdateInput, @Nonnull final Urn actor) {
    final Collection<MetadataChangeProposal> proposals = new ArrayList<>(6);
    final UpdateMappingHelper updateMappingHelper = new UpdateMappingHelper(DATASET_ENTITY_NAME);
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    if (datasetUpdateInput.getOwnership() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              OwnershipUpdateMapper.map(datasetUpdateInput.getOwnership(), actor),
              OWNERSHIP_ASPECT_NAME));
    }

    if (datasetUpdateInput.getDeprecation() != null) {
      final DatasetDeprecation deprecation = new DatasetDeprecation();
      deprecation.setDeprecated(datasetUpdateInput.getDeprecation().getDeprecated());
      if (datasetUpdateInput.getDeprecation().getDecommissionTime() != null) {
        deprecation.setDecommissionTime(datasetUpdateInput.getDeprecation().getDecommissionTime());
      }
      deprecation.setNote(datasetUpdateInput.getDeprecation().getNote());
      deprecation.setActor(actor, SetMode.IGNORE_NULL);
      proposals.add(
          updateMappingHelper.aspectToProposal(deprecation, DATASET_DEPRECATION_ASPECT_NAME));
    }

    if (datasetUpdateInput.getInstitutionalMemory() != null) {
      proposals.add(
          updateMappingHelper.aspectToProposal(
              InstitutionalMemoryUpdateMapper.map(datasetUpdateInput.getInstitutionalMemory()),
              INSTITUTIONAL_MEMORY_ASPECT_NAME));
    }

    if (datasetUpdateInput.getTags() != null || datasetUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (datasetUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(
            new TagAssociationArray(
                datasetUpdateInput.getGlobalTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(element))
                    .collect(Collectors.toList())));
      } else {
        // Tags field overrides deprecated globalTags field
        globalTags.setTags(
            new TagAssociationArray(
                datasetUpdateInput.getTags().getTags().stream()
                    .map(element -> TagAssociationUpdateMapper.map(element))
                    .collect(Collectors.toList())));
      }
      proposals.add(updateMappingHelper.aspectToProposal(globalTags, GLOBAL_TAGS_ASPECT_NAME));
    }

    if (datasetUpdateInput.getEditableSchemaMetadata() != null) {
      final EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
      editableSchemaMetadata.setEditableSchemaFieldInfo(
          new EditableSchemaFieldInfoArray(
              datasetUpdateInput.getEditableSchemaMetadata().getEditableSchemaFieldInfo().stream()
                  .map(element -> mapSchemaFieldInfo(element))
                  .collect(Collectors.toList())));
      editableSchemaMetadata.setLastModified(auditStamp);
      editableSchemaMetadata.setCreated(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableSchemaMetadata, EDITABLE_SCHEMA_METADATA_ASPECT_NAME));
    }

    if (datasetUpdateInput.getEditableProperties() != null) {
      final EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties();
      editableDatasetProperties.setDescription(
          datasetUpdateInput.getEditableProperties().getDescription());
      editableDatasetProperties.setLastModified(auditStamp);
      editableDatasetProperties.setCreated(auditStamp);
      proposals.add(
          updateMappingHelper.aspectToProposal(
              editableDatasetProperties, EDITABLE_DATASET_PROPERTIES_ASPECT_NAME));
    }

    return proposals;
  }

  private EditableSchemaFieldInfo mapSchemaFieldInfo(
      final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfoUpdate schemaFieldInfo) {
    final EditableSchemaFieldInfo output = new EditableSchemaFieldInfo();

    if (schemaFieldInfo.getDescription() != null) {
      output.setDescription(schemaFieldInfo.getDescription());
    }
    output.setFieldPath(schemaFieldInfo.getFieldPath());

    if (schemaFieldInfo.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(
          new TagAssociationArray(
              schemaFieldInfo.getGlobalTags().getTags().stream()
                  .map(element -> TagAssociationUpdateMapper.map(element))
                  .collect(Collectors.toList())));
      output.setGlobalTags(globalTags);
    }

    return output;
  }
}
