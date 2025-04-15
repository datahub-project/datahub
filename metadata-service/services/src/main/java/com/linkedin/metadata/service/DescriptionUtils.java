package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.linkedin.chart.EditableChartProperties;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.dashboard.EditableDashboardProperties;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.EditableMLModelProperties;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
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

  public static MetadataChangeProposal createContainerDescriptionChangeProposal(
      @Nonnull final EditableContainerProperties editableContainerProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableContainerProperties, "editableContainerProperties cannot be null");

    editableContainerProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.CONTAINER_ENTITY_NAME);
    proposal.setAspectName(Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableContainerProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createChartDescriptionChangeProposal(
      @Nonnull final EditableChartProperties editableChartProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableChartProperties, "editableChartProperties cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));

    editableChartProperties.setDescription(description);
    editableChartProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.CHART_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_CHART_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableChartProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDashboardDescriptionChangeProposal(
      @Nonnull final EditableDashboardProperties editableDashboardProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableDashboardProperties, "editableDashboardProperties cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));

    editableDashboardProperties.setDescription(description);
    editableDashboardProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DASHBOARD_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableDashboardProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDomainDescriptionChangeProposal(
      @Nonnull final DomainProperties domainProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(domainProperties, "domainProperties cannot be null");

    domainProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(domainProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createMLFeatureDescriptionChangeProposal(
      @Nonnull final EditableMLFeatureProperties editableMLFeatureProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableMLFeatureProperties, "editableMLFeatureProperties cannot be null");

    editableMLFeatureProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.ML_FEATURE_ENTITY_NAME);
    proposal.setAspectName(Constants.ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableMLFeatureProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createMLFeatureTableDescriptionChangeProposal(
      @Nonnull final EditableMLFeatureTableProperties editableMLFeatureTableProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableMLFeatureTableProperties, "editableMLFeatureTableProperties cannot be null");

    editableMLFeatureTableProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.ML_FEATURE_TABLE_ENTITY_NAME);
    proposal.setAspectName(Constants.ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableMLFeatureTableProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createMLModelDescriptionChangeProposal(
      @Nonnull final EditableMLModelProperties editableMLModelProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableMLModelProperties, "editableMLModelProperties cannot be null");

    editableMLModelProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.ML_MODEL_ENTITY_NAME);
    proposal.setAspectName(Constants.ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableMLModelProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createMLModelGroupDescriptionChangeProposal(
      @Nonnull final EditableMLModelGroupProperties editableMLModelGroupProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableMLModelGroupProperties, "editableMLModelGroupProperties cannot be null");

    editableMLModelGroupProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.ML_MODEL_GROUP_ENTITY_NAME);
    proposal.setAspectName(Constants.ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableMLModelGroupProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createMLPrimaryKeyDescriptionChangeProposal(
      @Nonnull final EditableMLPrimaryKeyProperties editableMLPrimaryKeyProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(
        editableMLPrimaryKeyProperties, "editableMLPrimaryKeyProperties cannot be null");

    editableMLPrimaryKeyProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.ML_PRIMARY_KEY_ENTITY_NAME);
    proposal.setAspectName(Constants.ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableMLPrimaryKeyProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDataFlowDescriptionChangeProposal(
      @Nonnull final EditableDataFlowProperties editableDataFlowProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableDataFlowProperties, "editableDataFlowProperties cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));

    editableDataFlowProperties.setDescription(description);
    editableDataFlowProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DATA_FLOW_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableDataFlowProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDataJobDescriptionChangeProposal(
      @Nonnull final EditableDataJobProperties editableDataJobProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(editableDataJobProperties, "editableDataJobProperties cannot be null");

    AuditStamp auditStamp = getAuditStamp(UrnUtils.getUrn(actor.toUrnStr()));

    editableDataJobProperties.setDescription(description);
    editableDataJobProperties.setLastModified(auditStamp);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DATA_JOB_ENTITY_NAME);
    proposal.setAspectName(Constants.EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(editableDataJobProperties));
    proposal.setChangeType(ChangeType.UPSERT);

    return proposal;
  }

  public static MetadataChangeProposal createDataProductDescriptionChangeProposal(
      @Nonnull final DataProductProperties dataProductProperties,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      @Nonnull final Actor actor) {
    Objects.requireNonNull(dataProductProperties, "dataProductProperties cannot be null");

    dataProductProperties.setDescription(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.DATA_PRODUCT_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dataProductProperties));
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
