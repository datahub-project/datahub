package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

public class DescriptionUtils {

  private DescriptionUtils() {}

  public static MetadataChangeProposal createGlossaryNodeDescriptionChangeProposal(
      @Nonnull final GlossaryNodeInfo glossaryNodeInfo,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description) {
    Objects.requireNonNull(glossaryNodeInfo, "glossaryNodeInfo cannot be null");

    glossaryNodeInfo.setDefinition(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(glossaryNodeInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createGlossaryTermDescriptionChangeProposal(
      @Nonnull final GlossaryTermInfo glossaryTermInfo,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description) {
    Objects.requireNonNull(glossaryTermInfo, "glossaryTermInfo cannot be null");

    glossaryTermInfo.setDefinition(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(glossaryTermInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDatasetDescriptionChangeProposal(
      @Nonnull final EditableDatasetProperties editableDatasetProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableDatasetProperties, "editableDatasetProperties cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));
    editableDatasetProperties.setDescription(description);
    editableDatasetProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableDatasetProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createSchemaFieldDescriptionChangeProposal(
      @Nonnull final EditableSchemaMetadata editableSchemaMetadata,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String fieldPath,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableSchemaMetadata, "editableSchemaMetadata cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));

    getFieldInfoFromSchema(editableSchemaMetadata, fieldPath).setDescription(description);

    editableSchemaMetadata.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableSchemaMetadata));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  // copied from graphql-core/.../MutationUtils
  static EditableSchemaFieldInfo getFieldInfoFromSchema(
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

  public static AuditStamp getAuditStamp(Urn actor) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
  }
}
