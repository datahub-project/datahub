package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.EditableMLModelProperties;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
import com.linkedin.notebook.EditableNotebookProperties;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.tag.TagProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DescriptionUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  private DescriptionUtils() {}

  public static void updateDatasetDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {

    EditableDatasetProperties editableDatasetProperties =
        (EditableDatasetProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableDatasetProperties());

    editableDatasetProperties.setDescription(newDescription);

    persistAspect(
        opContext,
        resourceUrn,
        Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
        editableDatasetProperties,
        actor,
        entityService);
  }

  public static void updateFieldDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      String fieldPath,
      Urn actor,
      EntityService<?> entityService) {
    EditableSchemaMetadata editableSchemaMetadata =
        (EditableSchemaMetadata)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                entityService,
                new EditableSchemaMetadata());
    EditableSchemaFieldInfo editableFieldInfo =
        getFieldInfoFromSchema(editableSchemaMetadata, fieldPath);

    editableFieldInfo.setDescription(newDescription);

    persistAspect(
        opContext,
        resourceUrn,
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        editableSchemaMetadata,
        actor,
        entityService);
  }

  public static void updateContainerDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableContainerProperties containerProperties =
        (EditableContainerProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableContainerProperties());
    containerProperties.setDescription(newDescription);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME,
        containerProperties,
        actor,
        entityService);
  }

  public static void updateDomainDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    DomainProperties domainProperties =
        (DomainProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
                entityService,
                null);
    if (domainProperties == null) {
      // If there are no properties for the domain already, then we should throw since the
      // properties model also requires a name.
      throw new IllegalArgumentException("Properties for this Domain do not yet exist!");
    }
    domainProperties.setDescription(newDescription);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
        domainProperties,
        actor,
        entityService);
  }

  public static void updateTagDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    TagProperties tagProperties =
        (TagProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.TAG_PROPERTIES_ASPECT_NAME,
                entityService,
                null);
    if (tagProperties == null) {
      tagProperties =
          new TagProperties().setName(resourceUrn.getId()).setDescription(newDescription);
    } else {
      tagProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.TAG_PROPERTIES_ASPECT_NAME,
        tagProperties,
        actor,
        entityService);
  }

  public static void updateCorpGroupDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    CorpGroupEditableInfo corpGroupEditableInfo =
        (CorpGroupEditableInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME,
                entityService,
                new CorpGroupEditableInfo());
    if (corpGroupEditableInfo != null) {
      corpGroupEditableInfo.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME,
        corpGroupEditableInfo,
        actor,
        entityService);
  }

  public static void updateGlossaryTermDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    GlossaryTermInfo glossaryTermInfo =
        (GlossaryTermInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                entityService,
                null);
    if (glossaryTermInfo == null) {
      // If there are no properties for the term already, then we should throw since the properties
      // model also requires a name.
      throw new IllegalArgumentException("Properties for this Glossary Term do not yet exist!");
    }
    glossaryTermInfo.setDefinition(
        newDescription); // We call description 'definition' for glossary terms. Not great, we know.
    // :(
    persistAspect(
        opContext,
        resourceUrn,
        Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
        glossaryTermInfo,
        actor,
        entityService);
  }

  public static void updateGlossaryNodeDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    GlossaryNodeInfo glossaryNodeInfo =
        (GlossaryNodeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
                entityService,
                null);
    if (glossaryNodeInfo == null) {
      throw new IllegalArgumentException("Glossary Node does not exist");
    }
    glossaryNodeInfo.setDefinition(newDescription);
    persistAspect(
        opContext,
        resourceUrn,
        Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
        glossaryNodeInfo,
        actor,
        entityService);
  }

  public static void updateNotebookDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableNotebookProperties notebookProperties =
        (EditableNotebookProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME,
                entityService,
                null);
    if (notebookProperties != null) {
      notebookProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME,
        notebookProperties,
        actor,
        entityService);
  }

  public static Boolean validateDescriptionInput(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    return true;
  }

  public static Boolean validateFieldDescriptionInput(
      @Nonnull OperationContext opContext,
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    validateSubresourceExists(opContext, resourceUrn, subResource, subResourceType, entityService);

    return true;
  }

  public static Boolean validateDomainInput(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    return true;
  }

  public static Boolean validateContainerInput(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    return true;
  }

  public static Boolean validateLabelInput(
      @Nonnull OperationContext opContext, Urn resourceUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, resourceUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }
    return true;
  }

  public static Boolean validateCorpGroupInput(
      @Nonnull OperationContext opContext, Urn corpUserUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, corpUserUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", corpUserUrn, corpUserUrn));
    }
    return true;
  }

  public static Boolean validateNotebookInput(
      @Nonnull OperationContext opContext, Urn notebookUrn, EntityService<?> entityService) {
    if (!entityService.exists(opContext, notebookUrn, true)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", notebookUrn, notebookUrn));
    }
    return true;
  }

  public static boolean isAuthorizedToUpdateFieldDescription(
      @Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToUpdateContainerDescription(
      @Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToUpdateDescription(
      @Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_ASSET_SUMMARY_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static void updateMlModelDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableMLModelProperties editableProperties =
        (EditableMLModelProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableMLModelProperties());
    if (editableProperties != null) {
      editableProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME,
        editableProperties,
        actor,
        entityService);
  }

  public static void updateMlModelGroupDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableMLModelGroupProperties editableProperties =
        (EditableMLModelGroupProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableMLModelGroupProperties());
    if (editableProperties != null) {
      editableProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME,
        editableProperties,
        actor,
        entityService);
  }

  public static void updateMlFeatureDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableMLFeatureProperties editableProperties =
        (EditableMLFeatureProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableMLFeatureProperties());
    if (editableProperties != null) {
      editableProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME,
        editableProperties,
        actor,
        entityService);
  }

  public static void updateMlFeatureTableDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableMLFeatureTableProperties editableProperties =
        (EditableMLFeatureTableProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableMLFeatureTableProperties());
    if (editableProperties != null) {
      editableProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME,
        editableProperties,
        actor,
        entityService);
  }

  public static void updateMlPrimaryKeyDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    EditableMLPrimaryKeyProperties editableProperties =
        (EditableMLPrimaryKeyProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME,
                entityService,
                new EditableMLPrimaryKeyProperties());
    if (editableProperties != null) {
      editableProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME,
        editableProperties,
        actor,
        entityService);
  }

  public static void updateDataProductDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    DataProductProperties properties =
        (DataProductProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                entityService,
                new DataProductProperties());
    if (properties != null) {
      properties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        properties,
        actor,
        entityService);
  }

  public static void updateApplicationDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    ApplicationProperties applicationProperties =
        (ApplicationProperties)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.APPLICATION_PROPERTIES_ASPECT_NAME,
                entityService,
                new ApplicationProperties());
    if (applicationProperties != null) {
      applicationProperties.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.APPLICATION_PROPERTIES_ASPECT_NAME,
        applicationProperties,
        actor,
        entityService);
  }

  public static void updateBusinessAttributeDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    BusinessAttributeInfo businessAttributeInfo =
        (BusinessAttributeInfo)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
                entityService,
                new BusinessAttributeInfo());
    if (businessAttributeInfo != null) {
      businessAttributeInfo.setDescription(newDescription);
    }
    persistAspect(
        opContext,
        resourceUrn,
        Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
        businessAttributeInfo,
        actor,
        entityService);
  }

  public static final String SOURCE_DETAIL_KEY_UI = "ui";
  public static final String SOURCE_DETAIL_KEY_PROPAGATED = "propagated";
  public static final String SOURCE_DETAIL_KEY_INFERRED = "inferred";
  public static final String SOURCE_DETAIL_VALUE_TRUE = "true";

  /**
   * Checks if a DocumentationAssociation was authored via the UI. A doc is considered UI-authored
   * if it has sourceDetail with "ui" = "true".
   */
  private static boolean isUiAuthoredDocumentation(DocumentationAssociation doc) {
    if (!doc.hasAttribution() || !doc.getAttribution().hasSourceDetail()) {
      return false;
    }
    return SOURCE_DETAIL_VALUE_TRUE.equals(
        doc.getAttribution().getSourceDetail().get(SOURCE_DETAIL_KEY_UI));
  }

  /**
   * Checks if a DocumentationAssociation is propagated. A doc is considered propagated if it has
   * sourceDetail with "propagated" = "true".
   */
  private static boolean isPropagatedDocumentation(DocumentationAssociation doc) {
    if (!doc.hasAttribution() || !doc.getAttribution().hasSourceDetail()) {
      return false;
    }
    return "true".equals(doc.getAttribution().getSourceDetail().get(SOURCE_DETAIL_KEY_PROPAGATED));
  }

  /**
   * Checks if a DocumentationAssociation is inferred (AI-generated). A doc is considered inferred
   * if it has sourceDetail with "inferred" = "true".
   */
  private static boolean isInferredDocumentation(DocumentationAssociation doc) {
    if (!doc.hasAttribution() || !doc.getAttribution().hasSourceDetail()) {
      return false;
    }
    return "true".equals(doc.getAttribution().getSourceDetail().get(SOURCE_DETAIL_KEY_INFERRED));
  }

  public static void updateDocumentDescription(
      @Nonnull OperationContext opContext,
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService<?> entityService) {
    // Get existing documentation aspect or create new
    Documentation documentation =
        (Documentation)
            EntityUtils.getAspectFromEntity(
                opContext,
                resourceUrn.toString(),
                Constants.DOCUMENTATION_ASPECT_NAME,
                entityService,
                new Documentation());

    // Create new documentation association with attribution
    // Label this as UI-authored so we can identify it later
    long currentTime = System.currentTimeMillis();
    MetadataAttribution attribution =
        new MetadataAttribution()
            .setTime(currentTime)
            .setActor(actor)
            .setSourceDetail(new StringMap(Map.of(SOURCE_DETAIL_KEY_UI, SOURCE_DETAIL_VALUE_TRUE)));

    DocumentationAssociation newDocAssociation =
        new DocumentationAssociation().setDocumentation(newDescription).setAttribution(attribution);

    // Keep all non-UI-authored documentation entries (propagated, inferred, and ingested)
    // This ensures we don't accidentally drop propagated descriptions from lineage or AI inferred
    // ones
    DocumentationAssociationArray associations =
        documentation.hasDocumentations()
            ? new DocumentationAssociationArray(
                documentation.getDocumentations().stream()
                    .filter(el -> !isUiAuthoredDocumentation(el))
                    .collect(java.util.stream.Collectors.toList()))
            : new DocumentationAssociationArray();

    // Add the new UI-authored documentation
    associations.add(newDocAssociation);
    documentation.setDocumentations(associations);

    persistAspect(
        opContext,
        resourceUrn,
        Constants.DOCUMENTATION_ASPECT_NAME,
        documentation,
        actor,
        entityService);
  }
}
