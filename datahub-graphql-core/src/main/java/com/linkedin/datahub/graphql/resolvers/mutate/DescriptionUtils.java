package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableList;

import com.linkedin.common.urn.Urn;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.domain.DomainProperties;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.ml.metadata.EditableMLFeatureProperties;
import com.linkedin.ml.metadata.EditableMLFeatureTableProperties;
import com.linkedin.ml.metadata.EditableMLModelGroupProperties;
import com.linkedin.ml.metadata.EditableMLModelProperties;
import com.linkedin.ml.metadata.EditableMLPrimaryKeyProperties;
import com.linkedin.notebook.EditableNotebookProperties;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.tag.TagProperties;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;


@Slf4j
public class DescriptionUtils {
  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  private DescriptionUtils() { }

  public static final String EDITABLE_SCHEMA_METADATA = "editableSchemaMetadata";

  public static void updateFieldDescription(
      String newDescription,
      Urn resourceUrn,
      String fieldPath,
      Urn actor,
      EntityService entityService
  ) {
      EditableSchemaMetadata editableSchemaMetadata =
          (EditableSchemaMetadata) getAspectFromEntity(
              resourceUrn.toString(), EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, fieldPath);

      editableFieldInfo.setDescription(newDescription);

      persistAspect(resourceUrn, EDITABLE_SCHEMA_METADATA, editableSchemaMetadata, actor, entityService);
  }

  public static void updateContainerDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    EditableContainerProperties containerProperties =
        (EditableContainerProperties) getAspectFromEntity(
            resourceUrn.toString(), Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableContainerProperties());
    containerProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME, containerProperties, actor, entityService);
  }

  public static void updateDomainDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    DomainProperties domainProperties =
        (DomainProperties) getAspectFromEntity(
            resourceUrn.toString(), Constants.DOMAIN_PROPERTIES_ASPECT_NAME, entityService, null);
    if (domainProperties == null) {
      // If there are no properties for the domain already, then we should throw since the properties model also requires a name.
      throw new IllegalArgumentException("Properties for this Domain do not yet exist!");
    }
    domainProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.DOMAIN_PROPERTIES_ASPECT_NAME, domainProperties, actor, entityService);
  }

  public static void updateTagDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    TagProperties tagProperties =
        (TagProperties) getAspectFromEntity(
            resourceUrn.toString(), Constants.TAG_PROPERTIES_ASPECT_NAME, entityService, null);
    if (tagProperties == null) {
      // If there are no properties for the tag already, then we should throw since the properties model also requires a name.
      throw new IllegalArgumentException("Properties for this Tag do not yet exist!");
    }
    tagProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.TAG_PROPERTIES_ASPECT_NAME, tagProperties, actor, entityService);
  }

  public static void updateCorpGroupDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    CorpGroupEditableInfo corpGroupEditableInfo =
        (CorpGroupEditableInfo) getAspectFromEntity(
            resourceUrn.toString(), Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME, entityService, new CorpGroupEditableInfo());
    corpGroupEditableInfo.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME, corpGroupEditableInfo, actor, entityService);
  }

  public static void updateGlossaryTermDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    GlossaryTermInfo glossaryTermInfo = (GlossaryTermInfo) getAspectFromEntity(
        resourceUrn.toString(), Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, entityService, null);
    if (glossaryTermInfo == null) {
      // If there are no properties for the term already, then we should throw since the properties model also requires a name.
      throw new IllegalArgumentException("Properties for this Glossary Term do not yet exist!");
    }
    glossaryTermInfo.setDefinition(newDescription); // We call description 'definition' for glossary terms. Not great, we know. :(
    persistAspect(resourceUrn, Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, glossaryTermInfo, actor, entityService);
  }

  public static void updateGlossaryNodeDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService
  ) {
    GlossaryNodeInfo glossaryNodeInfo = (GlossaryNodeInfo) getAspectFromEntity(
        resourceUrn.toString(), Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, entityService, null);
    if (glossaryNodeInfo == null) {
      throw new IllegalArgumentException("Glossary Node does not exist");
    }
    glossaryNodeInfo.setDefinition(newDescription);
    persistAspect(resourceUrn, Constants.GLOSSARY_NODE_INFO_ASPECT_NAME, glossaryNodeInfo, actor, entityService);
  }

  public static void updateNotebookDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableNotebookProperties notebookProperties = (EditableNotebookProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME, entityService, null);
    notebookProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME, notebookProperties, actor, entityService);
  }

  public static Boolean validateFieldDescriptionInput(
      Urn resourceUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    validateSubresourceExists(resourceUrn, subResource, subResourceType, entityService);

    return true;
  }

  public static Boolean validateDomainInput(
      Urn resourceUrn,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    return true;
  }

  public static Boolean validateContainerInput(
      Urn resourceUrn,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }

    return true;
  }

  public static Boolean validateLabelInput(
      Urn resourceUrn,
      EntityService entityService
  ) {
    if (!entityService.exists(resourceUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", resourceUrn, resourceUrn));
    }
    return true;
  }

  public static Boolean validateCorpGroupInput(
      Urn corpUserUrn,
      EntityService entityService
  ) {
    if (!entityService.exists(corpUserUrn)) {
      throw new IllegalArgumentException(String.format("Failed to update %s. %s does not exist.", corpUserUrn, corpUserUrn));
    }
    return true;
  }

  public static Boolean validateNotebookInput(
      Urn notebookUrn,
      EntityService entityService) {
    if (!entityService.exists(notebookUrn)) {
      throw new IllegalArgumentException(
          String.format("Failed to update %s. %s does not exist.", notebookUrn, notebookUrn));
    }
    return true;
  }

  public static boolean isAuthorizedToUpdateFieldDescription(@Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_DATASET_COL_DESCRIPTION_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToUpdateDomainDescription(@Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToUpdateContainerDescription(@Nonnull QueryContext context, Urn targetUrn) {
      final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
          ALL_PRIVILEGES_GROUP,
          new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType()))
      ));

      return AuthorizationUtils.isAuthorized(
          context.getAuthorizer(),
          context.getActorUrn(),
          targetUrn.getEntityType(),
          targetUrn.toString(),
          orPrivilegeGroups);
    }

  public static boolean isAuthorizedToUpdateDescription(@Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static void updateMlModelDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableMLModelProperties editableProperties = (EditableMLModelProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableMLModelProperties());
    editableProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME, editableProperties, actor, entityService);
  }

  public static void updateMlModelGroupDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableMLModelGroupProperties editableProperties = (EditableMLModelGroupProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableMLModelGroupProperties());
    editableProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME, editableProperties, actor, entityService);
  }
  public static void updateMlFeatureDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableMLFeatureProperties editableProperties = (EditableMLFeatureProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableMLFeatureProperties());
    editableProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME, editableProperties, actor, entityService);
  }

  public static void updateMlFeatureTableDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableMLFeatureTableProperties editableProperties = (EditableMLFeatureTableProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableMLFeatureTableProperties());
    editableProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME, editableProperties, actor, entityService);
  }

  public static void updateMlPrimaryKeyDescription(
      String newDescription,
      Urn resourceUrn,
      Urn actor,
      EntityService entityService) {
    EditableMLPrimaryKeyProperties editableProperties = (EditableMLPrimaryKeyProperties) getAspectFromEntity(
        resourceUrn.toString(), Constants.ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME, entityService, new EditableMLPrimaryKeyProperties());
    editableProperties.setDescription(newDescription);
    persistAspect(resourceUrn, Constants.ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME, editableProperties, actor, entityService);
  }
}
