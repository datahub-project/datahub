package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import javax.annotation.Nonnull;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;

import java.util.stream.Collectors;

public class DatasetUpdateInputSnapshotMapper implements InputModelMapper<DatasetUpdateInput, DatasetSnapshot, Urn> {

  public static final DatasetUpdateInputSnapshotMapper INSTANCE = new DatasetUpdateInputSnapshotMapper();

  public static DatasetSnapshot map(
      @Nonnull final DatasetUpdateInput datasetUpdateInput,
      @Nonnull final Urn actor) {
    return INSTANCE.apply(datasetUpdateInput, actor);
  }

  @Override
  public DatasetSnapshot apply(
      @Nonnull final DatasetUpdateInput datasetUpdateInput,
      @Nonnull final Urn actor) {
    final DatasetSnapshot result = new DatasetSnapshot();
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    final DatasetAspectArray aspects = new DatasetAspectArray();

    if (datasetUpdateInput.getOwnership() != null) {
      aspects.add(DatasetAspect.create(
          OwnershipUpdateMapper.map(datasetUpdateInput.getOwnership(), actor)));
    }

    if (datasetUpdateInput.getDeprecation() != null) {
      final DatasetDeprecation deprecation = new DatasetDeprecation();
      deprecation.setDeprecated(datasetUpdateInput.getDeprecation().getDeprecated());
      if (datasetUpdateInput.getDeprecation().getDecommissionTime() != null) {
        deprecation.setDecommissionTime(datasetUpdateInput.getDeprecation().getDecommissionTime());
      }
      deprecation.setNote(datasetUpdateInput.getDeprecation().getNote());
      deprecation.setActor(actor, SetMode.IGNORE_NULL);
      aspects.add(DatasetAspect.create(deprecation));
    }

    if (datasetUpdateInput.getInstitutionalMemory() != null) {
      aspects.add(DatasetAspect.create(InstitutionalMemoryUpdateMapper.map(datasetUpdateInput.getInstitutionalMemory())));
    }

    if (datasetUpdateInput.getTags() != null || datasetUpdateInput.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      if (datasetUpdateInput.getGlobalTags() != null) {
        globalTags.setTags(new TagAssociationArray(datasetUpdateInput.getGlobalTags()
            .getTags()
            .stream()
            .map(element -> TagAssociationUpdateMapper.map(element))
            .collect(Collectors.toList())));
      } else {
        // Tags field overrides deprecated globalTags field
        globalTags.setTags(new TagAssociationArray(datasetUpdateInput.getTags()
            .getTags()
            .stream()
            .map(element -> TagAssociationUpdateMapper.map(element))
            .collect(Collectors.toList())));
      }
      aspects.add(DatasetAspect.create(globalTags));
    }

    if (datasetUpdateInput.getEditableSchemaMetadata() != null) {
      final EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
      editableSchemaMetadata.setEditableSchemaFieldInfo(
          new EditableSchemaFieldInfoArray(
              datasetUpdateInput.getEditableSchemaMetadata().getEditableSchemaFieldInfo().stream().map(
                  element -> mapSchemaFieldInfo(element)
              ).collect(Collectors.toList())));
      editableSchemaMetadata.setLastModified(auditStamp);
      editableSchemaMetadata.setCreated(auditStamp);
      aspects.add(DatasetAspect.create(editableSchemaMetadata));
    }

    if (datasetUpdateInput.getEditableProperties() != null) {
      final EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties();
      editableDatasetProperties.setDescription(datasetUpdateInput.getEditableProperties().getDescription());
      editableDatasetProperties.setLastModified(auditStamp);
      editableDatasetProperties.setCreated(auditStamp);
      aspects.add(DatasetAspect.create(editableDatasetProperties));
    }

    result.setAspects(aspects);

    return result;
  }

  private EditableSchemaFieldInfo mapSchemaFieldInfo(
      final com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfoUpdate schemaFieldInfo
  ) {
    final EditableSchemaFieldInfo output = new EditableSchemaFieldInfo();

    if (schemaFieldInfo.getDescription() != null) {
      output.setDescription(schemaFieldInfo.getDescription());
    }
    output.setFieldPath(schemaFieldInfo.getFieldPath());

    if (schemaFieldInfo.getGlobalTags() != null) {
      final GlobalTags globalTags = new GlobalTags();
      globalTags.setTags(new TagAssociationArray(schemaFieldInfo.getGlobalTags().getTags().stream().map(
          element -> TagAssociationUpdateMapper.map(element)).collect(Collectors.toList())));
      output.setGlobalTags(globalTags);
    }

    return output;
  }
}