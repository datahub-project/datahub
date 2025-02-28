package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MutationUtils {

  private MutationUtils() {}

  public static void persistAspect(
      @Nonnull OperationContext opContext,
      Urn urn,
      String aspectName,
      RecordTemplate aspect,
      Urn actor,
      EntityService entityService) {
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposalWithUrn(urn, aspectName, aspect);
    entityService.ingestProposal(opContext, proposal, EntityUtils.getAuditStamp(actor), false);
  }

  /**
   * Only intended for use from GraphQL mutations, executes a different flow indicating a request
   * sourced from the UI
   *
   * @param urn
   * @param aspectName
   * @param aspect
   * @return
   */
  public static MetadataChangeProposal buildMetadataChangeProposalWithUrn(
      Urn urn, String aspectName, RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    return setProposalProperties(proposal, urn.getEntityType(), aspectName, aspect);
  }

  /**
   * Only intended for use from GraphQL mutations, executes a different flow indicating a request
   * sourced from the UI
   *
   * @param entityKey
   * @param entityType
   * @param aspectName
   * @param aspect
   * @return
   */
  public static MetadataChangeProposal buildMetadataChangeProposalWithKey(
      RecordTemplate entityKey, String entityType, String aspectName, RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(entityKey));
    return setProposalProperties(proposal, entityType, aspectName, aspect);
  }

  private static MetadataChangeProposal setProposalProperties(
      MetadataChangeProposal proposal,
      String entityType,
      String aspectName,
      RecordTemplate aspect) {
    proposal.setEntityType(entityType);
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);

    // Assumes proposal is generated first from the builder methods above so SystemMetadata is empty
    SystemMetadata systemMetadata = createDefaultSystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    proposal.setSystemMetadata(systemMetadata);
    return proposal;
  }

  public static EditableSchemaFieldInfo getFieldInfoFromSchema(
      EditableSchemaMetadata editableSchemaMetadata, String fieldPath) {
    if (!editableSchemaMetadata.hasEditableSchemaFieldInfo()) {
      editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray());
    }
    EditableSchemaFieldInfoArray editableSchemaMetadataArray =
        editableSchemaMetadata.getEditableSchemaFieldInfo();
    Optional<EditableSchemaFieldInfo> fieldMetadata =
        editableSchemaMetadataArray.stream()
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
      @Nonnull OperationContext opContext,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService) {
    if (subResourceType.equals(SubResourceType.DATASET_FIELD)) {
      SchemaMetadata schemaMetadata =
          (SchemaMetadata)
              entityService.getAspect(
                  opContext, targetUrn, Constants.SCHEMA_METADATA_ASPECT_NAME, 0);

      if (schemaMetadata == null) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to update %s & field %s. %s has no schema.",
                targetUrn, subResource, targetUrn));
      }

      Optional<SchemaField> fieldMatch =
          schemaMetadata.getFields().stream()
              .filter(field -> field.getFieldPath().equals(subResource))
              .findFirst();

      if (!fieldMatch.isPresent()) {
        throw new IllegalArgumentException(
            String.format(
                "Failed to update %s & field %s. Field %s does not exist in the datasets schema.",
                targetUrn, subResource, subResource));
      }

      return true;
    }

    throw new IllegalArgumentException(
        String.format(
            "Failed to update %s. SubResourceType (%s) is not valid. Types supported: %s.",
            targetUrn, subResource, SubResourceType.values()));
  }
}
