package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MutationUtils {
  public static final String SCHEMA_ASPECT_NAME = "schemaMetadata";

  private MutationUtils() { }

  public static void persistAspect(Urn urn, String aspectName, RecordTemplate aspect, Urn actor, EntityService entityService) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    entityService.ingestProposal(proposal, getAuditStamp(actor));
  }

  public static MetadataChangeProposal buildMetadataChangeProposal(Urn urn, String aspectName, RecordTemplate aspect, Urn actor, EntityService entityService) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  public static RecordTemplate getAspectFromEntity(String entityUrn, String aspectName, EntityService entityService, RecordTemplate defaultValue) {
    try {
      RecordTemplate aspect = entityService.getAspect(
          Urn.createFromString(entityUrn),
          aspectName,
          0
      );

      if (aspect == null) {
        return defaultValue;
      }

      return aspect;
    } catch (Exception e) {
      log.error(
          "Error constructing aspect from entity. Entity: {} aspect: {}. Error: {}",
          entityUrn,
          aspectName,
          e.toString()
      );
      e.printStackTrace();
      return null;
    }
  }

  public static AuditStamp getAuditStamp(Urn actor) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
  }

  public static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata,
      String fieldPath
  ) {
    if (!editableSchemaMetadata.hasEditableSchemaFieldInfo()) {
      editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
    }
    EditableSchemaFieldInfoArray editableSchemaMetadataArray =
        editableSchemaMetadata.getEditableSchemaFieldInfo();
    Optional<EditableSchemaFieldInfo> fieldMetadata = editableSchemaMetadataArray
        .stream()
        .filter(fieldInfo -> fieldInfo.getFieldPath().equals(fieldPath))
        .findFirst();

    if (fieldMetadata.isPresent()) {
      return fieldMetadata.get();
    } else {
      EditableSchemaFieldInfo newFieldInfo = new EditableSchemaFieldInfo();
      newFieldInfo.setFieldPath(fieldPath);
      editableSchemaMetadataArray.add(newFieldInfo);
      return newFieldInfo;
    }
  }

  public static Boolean validateSubresourceExists(
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService
  ) {
    if (subResourceType.equals(SubResourceType.DATASET_FIELD)) {
      SchemaMetadata schemaMetadata = (SchemaMetadata) entityService.getAspect(targetUrn, SCHEMA_ASPECT_NAME, 0);

      if (schemaMetadata == null) {
        throw new IllegalArgumentException(
            String.format("Failed to update %s & field %s. %s has no schema.", targetUrn, subResource, targetUrn)
        );
      }

      Optional<SchemaField> fieldMatch =
          schemaMetadata.getFields().stream().filter(field -> field.getFieldPath().equals(subResource)).findFirst();

      if (!fieldMatch.isPresent()) {
        throw new IllegalArgumentException(String.format(
            "Failed to update %s & field %s. Field %s does not exist in the datasets schema.",
            targetUrn, subResource, subResource));
      }

      return true;
    }

    throw new IllegalArgumentException(String.format(
        "Failed to update %s. SubResourceType (%s) is not valid. Types supported: %s.",
        targetUrn, subResource, SubResourceType.values()
    ));
  }

}
